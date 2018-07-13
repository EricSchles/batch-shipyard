#!/usr/bin/env python3

# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# stdlib imports
import argparse
import asyncio
import concurrent.futures
import hashlib
import json
import logging
import logging.handlers
import multiprocessing
import pickle
import threading
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    Tuple,
)
# non-stdlib imports
import azure.batch
import azure.batch.models as batchmodels
import azure.cosmosdb.table
import azure.mgmt.compute
import azure.mgmt.resource
import azure.mgmt.storage
import azure.storage.blob
import azure.storage.queue
import msrestazure.azure_active_directory
import msrestazure.azure_cloud

# create logger
logger = logging.getLogger(__name__)
# global defines
_MAX_EXECUTOR_WORKERS = min((multiprocessing.cpu_count() * 4, 32))


def _setup_logger() -> None:
    # type: (None) -> None
    """Set up logger"""
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)sZ %(levelname)s %(name)s:%(funcName)s:%(lineno)d '
        '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def is_none_or_empty(obj: Any) -> bool:
    """Determine if object is None or empty
    :type obj: object
    :return: if object is None or empty
    """
    return obj is None or len(obj) == 0


def is_not_empty(obj: Any) -> bool:
    """Determine if object is not None and is length is > 0
    :type obj: object
    :return: if object is not None and length is > 0
    """
    return obj is not None and len(obj) > 0


def hash_string(strdata: str) -> str:
    """Hash a string
    :param strdata: string data to hash
    :return: hexdigest
    """
    return hashlib.sha1(strdata.encode('utf8')).hexdigest()


def hash_federation_id(federation_id: str) -> str:
    """Hash a federation id
    :param federation_id: federation id
    :return: hashed federation id
    """
    return hash_string(federation_id)


class Credentials():
    def __init__(self, config: Dict[str, Any]) -> None:
        """Ctor for Credentials
        :param config: configuration
        """
        # set attr from config
        self.storage_account = config['storage']['account']
        self.storage_account_rg = config['storage']['resource_group']
        # get cloud object
        self.cloud = Credentials.convert_cloud_type(config['aad_cloud'])
        # get aad creds
        self.arm_creds = self.create_msi_credentials()
        self.batch_creds = self.create_msi_credentials(
            resource_id=self.cloud.endpoints.batch_resource_id)
        # get subscription id
        self.sub_id = self.get_subscription_id()
        logger.debug('created msi auth for sub id: {}'.format(self.sub_id))
        # get storage account key and endpoint
        self.storage_account_key, self.storage_account_ep = \
            self.get_storage_account_key()
        logger.debug('storage account {} -> rg: {} ep: {}'.format(
            self.storage_account, self.storage_account_rg,
            self.storage_account_ep))

    @staticmethod
    def convert_cloud_type(cloud_type: str) -> msrestazure.azure_cloud.Cloud:
        """Convert clout type string to object
        :param cloud_type: cloud type to convert
        :return: cloud object
        """
        if cloud_type == 'public':
            cloud = msrestazure.azure_cloud.AZURE_PUBLIC_CLOUD
        elif cloud_type == 'china':
            cloud = msrestazure.azure_cloud.AZURE_CHINA_CLOUD
        elif cloud_type == 'germany':
            cloud = msrestazure.azure_cloud.AZURE_GERMAN_CLOUD
        elif cloud_type == 'usgov':
            cloud = msrestazure.azure_cloud.AZURE_US_GOV_CLOUD
        else:
            raise ValueError('unknown cloud_type: {}'.format(cloud_type))
        return cloud

    def get_subscription_id(self) -> str:
        """Get subscription id for ARM creds
        :param arm_creds: ARM creds
        :return: subscription id
        """
        client = azure.mgmt.resource.SubscriptionClient(self.arm_creds)
        return next(client.subscriptions.list()).subscription_id

    def create_msi_credentials(
            self,
            resource_id: str=None
    ) -> msrestazure.azure_active_directory.MSIAuthentication:
        """Create MSI credentials
        :param resource_id: resource id to auth against
        :return: MSI auth object
        """
        if is_not_empty(resource_id):
            creds = msrestazure.azure_active_directory.MSIAuthentication(
                cloud_environment=self.cloud,
                resource=resource_id,
            )
        else:
            creds = msrestazure.azure_active_directory.MSIAuthentication(
                cloud_environment=self.cloud,
            )
        return creds

    def get_storage_account_key(self) -> Tuple[str, str]:
        """Retrieve the storage account key and endpoint
        :return: tuple of key, endpoint
        """
        client = azure.mgmt.storage.StorageManagementClient(
            self.arm_creds, self.sub_id,
            base_url=self.cloud.endpoints.resource_manager)
        ep = None
        if is_not_empty(self.storage_account_rg):
            acct = client.storage_accounts.get_properties(
                self.storage_account_rg, self.storage_account)
            ep = '.'.join(
                acct.primary_endpoints.blob.rstrip('/').split('.')[2:]
            )
        else:
            for acct in client.storage_accounts.list():
                if acct.name == self.storage_account:
                    self.storage_account_rg = acct.id.split('/')[4]
                    ep = '.'.join(
                        acct.primary_endpoints.blob.rstrip('/').split('.')[2:]
                    )
                    break
        if is_none_or_empty(self.storage_account_rg) or is_none_or_empty(ep):
            raise RuntimeError(
                'storage account {} not found in subscription id {}'.format(
                    self.storage_account, self.sub_id))
        keys = client.storage_accounts.list_keys(
            self.storage_account_rg, self.storage_account)
        return (keys.keys[0].value, ep)


class ServiceProxy():
    def __init__(self, config: Dict[str, Any]) -> None:
        """Ctor for ServiceProxy
        :param config: configuration
        """
        self._batch_client_lock = threading.Lock()
        self.batch_clients = {}
        self.batch_shipyard_version = config['batch_shipyard']['version']
        prefix = config['storage']['entity_prefix']
        self.queue_prefix = '{}fed'.format(prefix)
        self.table_name_global = '{}fedglobal'.format(prefix)
        self.table_name_jobs = '{}fedjobs'.format(prefix)
        # create credentials
        self.creds = Credentials(config)
        # create clients
        self.compute_client = self._create_compute_client()
        self.table_client = self._create_table_client()
        self.queue_client = self._create_queue_client()
        logger.debug('created storage clients for storage account {}'.format(
            self.creds.storage_account))

    def _modify_client_for_retry_and_user_agent(self, client: Any) -> None:
        """Extend retry policy of clients and add user agent string
        :param client: a client object
        """
        if client is None:
            return
        client.config.retry_policy.max_backoff = 8
        client.config.retry_policy.retries = 100
        client.config.add_user_agent('batch-shipyard/{}'.format(
            self.batch_shipyard_version))

    def _create_table_client(self) -> azure.cosmosdb.table.TableService:
        """Create a table client for the given storage account
        :return: table client
        """
        client = azure.cosmosdb.table.TableService(
            account_name=self.creds.storage_account,
            account_key=self.creds.storage_account_key,
            endpoint_suffix=self.creds.storage_account_ep,
        )
        return client

    def _create_queue_client(self) -> azure.storage.queue.QueueService:
        """Create a queue client for the given storage account
        :return: queue client
        """
        client = azure.storage.queue.QueueService(
            account_name=self.creds.storage_account,
            account_key=self.creds.storage_account_key,
            endpoint_suffix=self.creds.storage_account_ep,
        )
        return client

    def _create_compute_client(
            self
    ) -> azure.mgmt.compute.ComputeManagementClient:
        """Create a compute mgmt client
        :return: compute client
        """
        client = azure.mgmt.compute.ComputeManagementClient(
            self.creds.arm_creds, self.creds.sub_id,
            base_url=self.creds.cloud.endpoints.resource_manager)
        return client

    def batch_client(
            self,
            batch_account: str,
            service_url: str
    ) -> azure.batch.BatchServiceClient:
        """Get/create batch client
        :param batch_account: batch account name
        :param service_url: service url
        :return: batch client
        """
        with self._batch_client_lock:
            try:
                return self.batch_clients[batch_account]
            except KeyError:
                client = azure.batch.BatchServiceClient(
                    self.creds.batch_creds, base_url=service_url)
                self._modify_client_for_retry_and_user_agent(client)
                self.batch_clients[batch_account] = client
                logger.debug('batch client created for account: {}'.format(
                    batch_account))
                return client


class ComputeServiceHandler():
    def __init__(self, service_proxy: ServiceProxy) -> None:
        """Ctor for Compute Service handler
        :param service_proxy: ServiceProxy
        """
        self.service_proxy = service_proxy
        self._vm_sizes_lock = threading.Lock()
        self._queried_locations = set()
        self._vm_sizes = {}

    def populate_vm_sizes_from_location(self, location: str) -> None:
        """Populate VM sizes for a location
        :param location: location
        """
        location = location.lower()
        with self._vm_sizes_lock:
            if location in self._queried_locations:
                return
        vmsizes = list(
            self.service_proxy.compute_client.virtual_machine_sizes.list(
                location)
        )
        with self._vm_sizes_lock:
            for vmsize in vmsizes:
                name = vmsize.name.lower()
                if name in self._vm_sizes:
                    continue
                self._vm_sizes[name] = vmsize
            self._queried_locations.add(location)

    def get_vm_size(
            self,
            vm_size: str
    ) -> 'azure.mgmt.compute.models.VirtualMachineSize':
        """Get VM Size information
        :param vm_size: name of VM size
        """
        with self._vm_sizes_lock:
            return self._vm_sizes[vm_size.lower()]


class BatchServiceHandler():
    def __init__(self, service_proxy: ServiceProxy) -> None:
        """Ctor for Federation Batch handler
        :param service_proxy: ServiceProxy
        """
        self.service_proxy = service_proxy

    def get_pool_full_update(
            self,
            batch_account: str,
            service_url: str,
            pool_id: str,
    ) -> batchmodels.CloudPool:
        client = self.service_proxy.batch_client(batch_account, service_url)
        try:
            return client.pool.get(pool_id)
        except batchmodels.BatchErrorException as e:
            logger.error(
                'could not retrieve Batch pool {} (account={} '
                'service_url={})'.format(pool_id, batch_account, service_url))

    def get_node_state_counts(
            self,
            batch_account: str,
            service_url: str,
            pool_id: str,
    ) -> batchmodels.PoolNodeCounts:
        client = self.service_proxy.batch_client(batch_account, service_url)
        try:
            node_counts = client.account.list_pool_node_counts(
                account_list_pool_node_counts_options=batchmodels.
                AccountListPoolNodeCountsOptions(
                    filter='poolId eq \'{}\''.format(pool_id)
                )
            )
            nc = list(node_counts)
            if len(nc) == 0:
                logger.error(
                    'no node counts for pool {} (account={} '
                    'service_url={})'.format(
                        pool_id, batch_account, service_url))
            return nc[0]
        except batchmodels.BatchErrorException as e:
            logger.error(
                'could not retrieve pool {} node counts (account={} '
                'service_url={})'.format(pool_id, batch_account, service_url))

    def get_job(
            self,
            batch_account: str,
            service_url: str,
            job_id: str,
    ) -> bool:
        client = self.service_proxy.batch_client(batch_account, service_url)
        client.job.get(job_id)

    def add_job(
            self,
            batch_account: str,
            service_url: str,
            job: batchmodels.JobAddParameter,
    ) -> bool:
        client = self.service_proxy.batch_client(batch_account, service_url)
        client.job.add(job)


def _submit_task_sub_collection(
        batch_client, job_id, start, end, slice, all_tasks, task_map):
    # type: (batch.BatchServiceClient, str, int, int, int, list, dict) -> None
    """Submits a sub-collection of tasks, do not call directly
    :param batch_client: The batch client to use.
    :type batch_client: `azure.batch.batch_service_client.BatchServiceClient`
    :param str job_id: job to add to
    :param int start: start offset, includsive
    :param int end: end offset, exclusive
    :param int slice: slice width
    :param list all_tasks: list of all task ids
    :param dict task_map: task collection map to add
    """
    initial_slice = slice
    while True:
        chunk_end = start + slice
        if chunk_end > end:
            chunk_end = end
        chunk = all_tasks[start:chunk_end]
        logger.debug('submitting {} tasks ({} -> {}) to job {}'.format(
            len(chunk), start, chunk_end - 1, job_id))
        try:
            results = batch_client.task.add_collection(job_id, chunk)
        except batchmodels.BatchErrorException as e:
            if e.error.code == 'RequestBodyTooLarge':
                # collection contents are too large, reduce and retry
                if slice == 1:
                    raise
                slice = slice >> 1
                if slice < 1:
                    slice = 1
                logger.error(
                    ('task collection slice was too big, retrying with '
                     'slice={}').format(slice))
                continue
        else:
            # go through result and retry just failed tasks
            while True:
                retry = []
                for result in results.value:
                    if result.status == batchmodels.TaskAddStatus.client_error:
                        de = [
                            '{}: {}'.format(x.key, x.value)
                            for x in result.error.values
                        ]
                        logger.error(
                            ('skipping retry of adding task {} as it '
                             'returned a client error (code={} message={} {}) '
                             'for job {}').format(
                                 result.task_id, result.error.code,
                                 result.error.message, ' '.join(de), job_id))
                    elif (result.status ==
                          batchmodels.TaskAddStatus.server_error):
                        retry.append(task_map[result.task_id])
                if len(retry) > 0:
                    logger.debug('retrying adding {} tasks to job {}'.format(
                        len(retry), job_id))
                    results = batch_client.task.add_collection(job_id, retry)
                else:
                    break
        if chunk_end == end:
            break
        start = chunk_end
        slice = initial_slice


def _add_task_collection(batch_client, job_id, task_map):
    # type: (batch.BatchServiceClient, str, dict) -> None
    """Add a collection of tasks to a job
    :param batch_client: The batch client to use.
    :type batch_client: `azure.batch.batch_service_client.BatchServiceClient`
    :param str job_id: job to add to
    :param dict task_map: task collection map to add
    """
    all_tasks = list(task_map.values())
    slice = 100  # can only submit up to 100 tasks at a time
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=_MAX_EXECUTOR_WORKERS) as executor:
        for start in range(0, len(all_tasks), slice):
            end = start + slice
            if end > len(all_tasks):
                end = len(all_tasks)
            executor.submit(
                _submit_task_sub_collection, batch_client, job_id, start, end,
                end - start, all_tasks, task_map)
    logger.info('submitted all {} tasks to job {}'.format(
        len(task_map), job_id))




class FederationDataHandler():
    _ALL_FEDERATIONS_PK = '!!FEDERATIONS'

    def __init__(self, service_proxy: ServiceProxy) -> None:
        """Ctor for Federation data handler
        :param service_proxy: ServiceProxy
        """
        self.service_proxy = service_proxy

    def get_all_federations(self) -> List[azure.cosmosdb.table.Entity]:
        """Get all federations"""
        return self.service_proxy.table_client.query_entities(
            self.service_proxy.table_name_global,
            filter='PartitionKey eq \'{}\''.format(self._ALL_FEDERATIONS_PK))

    def get_all_pools_for_federation(
            self,
            fedhash: str
    ) -> List[azure.cosmosdb.table.Entity]:
        """Get all pools for a federation
        :param fedhash: federation hash
        """
        return self.service_proxy.table_client.query_entities(
            self.service_proxy.table_name_global,
            filter='PartitionKey eq \'{}\''.format(fedhash))

    def get_messages_from_federation_queue(
            self,
            fedhash: str
    ) -> List[azure.storage.queue.models.QueueMessage]:
        queue_name = '{}-{}'.format(
            self.service_proxy.queue_prefix, fedhash)
        logger.debug('pulling messages from queue {}'.format(queue_name))
        return self.service_proxy.queue_client.get_messages(
            queue_name, num_messages=32, visibility_timeout=1)

    def delete_message_from_federation_queue(
            self,
            fedhash: str,
            msg_id: str,
            pop_receipt: str
    ) -> None:
        queue_name = '{}-{}'.format(
            self.service_proxy.queue_prefix, fedhash)
        self.service_proxy.queue_client.delete_message(
            queue_name, msg_id, pop_receipt)

    def _create_blob_client(self, sa, ep, sas):
        return azure.storage.blob.BlockBlobService(
            account_name=sa,
            sas_token=sas,
            endpoint_suffix=ep
        )

    def retrieve_blob_data(
            self,
            url: str
    ) -> Tuple[azure.storage.blob.BlockBlobService, str, str, bytes]:
        """Retrieve a blob URL
        :param url: Azure Storage url to retrieve
        :return: blob client, container, blob name, data
        """
        # explode url into parts
        tmp = url.split('/')
        host = tmp[2].split('.')
        sa = host[0]
        ep = '.'.join(host[2:])
        del host
        tmp = '/'.join(tmp[3:]).split('?')
        sas = tmp[1]
        tmp = tmp[0].split('/')
        container = tmp[0]
        blob_name = '/'.join(tmp[1:])
        del tmp
        blob_client = self._create_blob_client(sa, ep, sas)
        data = blob_client.get_blob_to_bytes(container, blob_name)
        return blob_client, container, blob_name, data.content

    def delete_blob(
            self,
            blob_client: azure.storage.blob.BlockBlobService,
            container: str,
            blob_name: str
    ) -> None:
        blob_client.delete_blob(container, blob_name)


class FederationPool():
    def __init__(
            self,
            batch_account: str,
            service_url: str,
            location: str,
            pool_id: str,
            cloud_pool: batchmodels.CloudPool,
            vm_size: 'azure.mgmt.compute.models.VirtualMachineSize'
    ) -> None:
        self.batch_account = batch_account
        self.service_url = service_url
        self.location = location
        self.pool_id = pool_id
        self.cloud_pool = cloud_pool
        self.node_counts = None
        self.node_counts_refresh_epoch = None
        self.vm_props = vm_size
        self._vm_size = None
        if self.is_valid:
            self._vm_size = self.cloud_pool.vm_size.lower()

    @property
    def is_valid(self) -> bool:
        if self.cloud_pool is not None and self.vm_props is not None:
            return self.cloud_pool.state == batchmodels.PoolState.active
        return False

    @property
    def vm_size(self) -> Optional[str]:
        return self._vm_size

    @property
    def is_internode_comm_enabled(self) -> bool:
        return self.cloud_pool.enable_inter_node_communication

    @property
    def is_gpu(self) -> bool:
        if self.vm_size.startswith('standard_n'):
            return True
        return False

    @property
    def is_rdma(self) -> bool:
        if (self.vm_size.endswith('r') or
                self.vm_size.endswith('rs') or
                self.vm_size.endswith('rs_v2') or
                self.vm_size.endswith('rs_v3') or
                self.vm_size == 'standard_a8' or
                self.vm_size == 'standard_a9'):
            return True
        return False

    @property
    def is_premium(self) -> bool:
        if (self.vm_size.endswith('s') or
                self.vm_size.endswith('s_v2') or
                self.vm_size.endswith('s_v3') or
                self.vm_size.startswith('standard_ds') or
                self.vm_size.startswith('standard_gs')):
            return True
        return False


class Federation():
    def __init__(self, fedhash, fedid):
        self.lock = threading.Lock()
        self.hash = fedhash
        self.id = fedid
        self.pools = {}

    def update_pool(
            self,
            csh: ComputeServiceHandler,
            bsh: BatchServiceHandler,
            entity: azure.cosmosdb.table.Entity
    ) -> str:
        with self.lock:
            rk = entity['RowKey']
            if rk in self.pools:
                return rk
            batch_account = entity['BatchAccount']
            poolid = entity['PoolId']
            service_url = entity['BatchServiceUrl']
            location = entity['Location']
            csh.populate_vm_sizes_from_location(location)
            pool = bsh.get_pool_full_update(
                batch_account, service_url, poolid)
            vm_size = None
            if pool is not None:
                vm_size = csh.get_vm_size(pool.vm_size)
            fedpool = FederationPool(
                batch_account, service_url, location, poolid, pool, vm_size
            )
            self.pools[rk] = fedpool
            if self.pools[rk].is_valid:
                logger.info(
                    'added valid pool {} id={} to federation {} id={} for '
                    'account {} at location {} size={} ppn={} mem={}'.format(
                        rk, poolid, self.hash, self.id, batch_account,
                        location, fedpool.vm_size,
                        fedpool.vm_props.number_of_cores,
                        fedpool.vm_props.memory_in_mb))
            else:
                logger.info(
                    'added invalid pool {} id={} to federation {} '
                    'id={}'.format(rk, poolid, self.hash, self.id))
            return rk

    def trim_orphaned_pools(self, fedpools: set) -> None:
        with self.lock:
            # do not get symmetric difference
            diff = [x for x in self.pools.keys() if x not in fedpools]
            for rk in diff:
                logger.debug(
                    'removing pool {} from federation {} id={}'.format(
                        rk, self.hash, self.id))
                self.pools.pop(rk)
            logger.info('active pools in federation {} id={}: {}'.format(
                self.hash, self.id, ' '.join(self.pools.keys())))

    def find_target_pool_for_job(
            self,
            bsh: BatchServiceHandler,
            job: batchmodels.JobAddParameter,
            num_tasks: int,
            blacklist: Set[str],
            unique_id: str
    ) -> Optional[str]:
        """
        This function should be called with lock already held!
        """
        # refresh all pools in federation
        avail_dedicated_vms = {}
        avail_lowpriority_vms = {}
        idle_dedicated_vms = {}
        idle_lowpriority_vms = {}
        for rk in self.pools:
            if rk in blacklist:
                continue
            pool = self.pools[rk]
            # refresh pool
            if not pool.is_valid:
                pool.cloud_pool = bsh.get_pool_full_update(
                    pool.batch_account, pool.service_url, pool.pool_id)
                if not pool.is_valid:
                    continue
            # TODO resource contraint filtering

            # TODO handle when there are no pools that can be matched

            # refresh node state counts
            if unique_id != pool.node_counts_refresh_epoch:
                pool.node_counts = bsh.get_node_state_counts(
                    pool.batch_account, pool.service_url, pool.pool_id)
                pool.node_counts_refresh_epoch = unique_id
            # add counts for pre-sort
            if pool.node_counts.dedicated.idle > 0:
                idle_dedicated_vms[rk] = pool.node_counts.dedicated.idle
            if pool.node_counts.low_priority.idle > 0:
                idle_lowpriority_vms[rk] = pool.node_counts.low_priority.idle
            avail_dedicated_vms[rk] = (
                pool.node_counts.dedicated.idle +
                pool.node_counts.dedicated.running
            )
            if avail_dedicated_vms[rk] == 0:
                avail_dedicated_vms.pop(rk)
            avail_lowpriority_vms[rk] = (
                pool.node_counts.low_priority.idle +
                pool.node_counts.low_priority.running
            )
            if avail_lowpriority_vms[rk] == 0:
                avail_lowpriority_vms.pop(rk)
        if len(idle_dedicated_vms) == 0 and len(idle_lowpriority_vms) == 0:
            if (len(avail_dedicated_vms) != 0 or
                    len(avail_lowpriority_vms) != 0):
                binned_avail_dedicated = sorted(
                    avail_dedicated_vms, key=avail_dedicated_vms.get)
                if len(binned_avail_dedicated) > 0:
                    return binned_avail_dedicated[0]
                binned_avail_lp = sorted(
                    avail_lowpriority_vms, key=avail_lowpriority_vms.get)
                return binned_avail_lp[0]
        else:
            binned_idle_dedicated = sorted(
                idle_dedicated_vms, key=idle_dedicated_vms.get)
            if len(binned_idle_dedicated) > 0:
                return binned_idle_dedicated[0]
            binned_idle_lp = sorted(
                idle_lowpriority_vms, key=idle_lowpriority_vms.get)
            return binned_idle_lp[0]
        return None

    def schedule_job(
            self,
            bsh: BatchServiceHandler,
            target_pool: str,
            job: batchmodels.JobAddParameter,
            constraints: Dict[str, Any],
            task_map: Dict[str, batchmodels.TaskAddParameter],
    ) -> bool:
        """
        This function should be called with lock already held!
        """
        multi_instance = constraints['has_multi_instance']
        auto_complete = constraints['auto_complete']
        # get pool ref
        pool = self.pools[target_pool]
        # add job
        try:
            logger.info('Adding job {} to pool {}'.format(
                job.id, pool.pool_id))
            bsh.add_job(pool.batch_account, pool.service_url, job)
        except batchmodels.batch_error.BatchErrorException as ex:
            if ('The specified job is already in a completed state.' in
                    ex.message.value):
                raise
            elif 'The specified job already exists' in ex.message.value:
                # cannot re-use an existing job if multi-instance due to
                # job release requirement
                if multi_instance and auto_complete:
                    raise
                else:
                    # retrieve job and check for version consistency
                    _job = bsh.get_job(
                        pool.batch_account, pool.service_url, job.id)
                    # check for task dependencies and job actions
                    # compatibility
                    if (job.uses_task_dependencies and
                            not _job.uses_task_dependencies):
                        raise RuntimeError(
                            ('existing job {} has an incompatible task '
                             'dependency setting: existing={} '
                             'desired={}').format(
                                 job.id, _job.uses_task_dependencies,
                                 job.uses_task_dependencies))
                    if (_job.on_task_failure != job.on_task_failure):
                        raise RuntimeError(
                            ('existing job {} has an incompatible '
                             'on_task_failure setting: existing={} '
                             'desired={}').format(
                                 job.id, _job.on_task_failure.value,
                                 job.on_task_failure.value))
            else:
                raise

        # TODO remap task ids given current job
        # TODO submit task collection
        raise RuntimeError()


class FederationProcessor():
    def __init__(self, config: Dict[str, Any]) -> None:
        """Ctor for FederationProcessor
        :param config: configuration
        """
        self._service_proxy = ServiceProxy(config)
        self.fed_refresh_interval = config.get(
            'federation_refresh_interval', 30)
        self.csh = ComputeServiceHandler(self._service_proxy)
        self.bsh = BatchServiceHandler(self._service_proxy)
        self.fdh = FederationDataHandler(self._service_proxy)
        # data structs
        self._federation_lock = threading.Lock()
        self.federations = {}  # type: Dict[str, Federation]

    @property
    def federations_available(self) -> bool:
        with self._federation_lock:
            return len(self.federations) > 0

    def update_federations(self):
        """Update federations"""
        logger.debug('starting federation update')
        entities = list(self.fdh.get_all_federations())
        with self._federation_lock:
            for entity in entities:
                fedhash = entity['RowKey']
                fedid = entity['FederationId']
                if fedhash not in self.federations:
                    logger.debug('adding federation hash {} id: {}'.format(
                        fedhash, fedid))
                    self.federations[fedhash] = Federation(fedhash, fedid)
                poolset = set()
                pools = self.fdh.get_all_pools_for_federation(fedhash)
                for pool in pools:
                    poolrk = self.federations[fedhash].update_pool(
                        self.csh, self.bsh, pool)
                    poolset.add(poolrk)
                self.federations[fedhash].trim_orphaned_pools(poolset)
        logger.debug('completed federation update')

    def add_job_v1(
            self, fedhash, job_id, job, constraints, task_map, unique_id):
        # get the number of tasks in job
        # try to match the appropriate pool for the tasks in job
        # add job to pool
        # if job exists, ensure settings match
        # add tasks to job
        # record mapping in fedjobs table
        num_tasks = len(task_map)
        logger.debug(
            'attempting to match job {} with {} tasks in fed {} '
            'constraints={}'.format(job_id, num_tasks, fedhash, constraints))
        blacklist = set()
        while True:
            poolrk = self.federations[fedhash].find_target_pool_for_job(
                self.bsh, job, num_tasks, blacklist, unique_id)
            if poolrk is not None:
                if self.federations[fedhash].schedule_job(
                        self.bsh, poolrk, job, constraints, task_map):
                    break
                else:
                    blacklist.add(poolrk)

    def process_message_action_v1(
            self, fedhash, action, target_type, data, unique_id):
        if target_type == 'job_schedule':
            if action == 'add':
                pass
            else:
                raise NotImplementedError()
        elif target_type == 'job':
            job_id = data['job']['id']
            if action == 'add':
                job = data['job']['data']
                constraints = data['job']['constraints']
                task_map = data['task_map']
                self.add_job_v1(
                    fedhash, job_id, job, constraints, task_map, unique_id)
            else:
                raise NotImplementedError()
        else:
            logger.error('unknown target type: {}'.format(target_type))

    def process_queue_message_v1(
            self,
            fedhash: str,
            msg: Dict[str, Any]
    ) -> None:
        target_fedid = msg['federation_id']
        calc_fedhash = hash_federation_id(target_fedid)
        if calc_fedhash != fedhash:
            logger.error(
                'federation hash mismatch, expected={} actual={} id={}'.format(
                    fedhash, calc_fedhash, target_fedid))
            return
        action = msg['action']['method']
        target_type = msg['action']['kind']
        job_data = None
        unique_id = None
        # retrieve job/task data
        if 'blob_data' in msg:
            blob_client, container, blob_name, data = \
                self.fdh.retrieve_blob_data(msg['blob_data'])
            job_data = pickle.loads(data, fix_imports=True)
            unique_id = blob_name.split('.')[0]
        # check version first
        if job_data['version'] == '1':
            self.process_message_action_v1(
                fedhash, action, target_type, job_data, unique_id)
        else:
            logger.error('cannot process job data version {} for {}'.format(
                job_data['version'], unique_id))
        # delete blob
        self.fdh.delete_blob(blob_client, container, blob_name)

    def process_federation_queue(self, fedhash: str) -> None:
        acquired = self.federations[fedhash].lock.acquire(blocking=False)
        if not acquired:
            logger.debug('could not acquire lock on federation {}'.format(
                fedhash))
            return
        try:
            msgs = self.fdh.get_messages_from_federation_queue(fedhash)
            for msg in msgs:
                msg_data = json.loads(msg.content, encoding='utf8')
                if msg_data['version'] == '1':
                    self.process_queue_message_v1(fedhash, msg_data)
                else:
                    logger.error(
                        'cannot process message version {} for fed {}'.format(
                            msg_data['version'], fedhash))
                # delete message
                self.fdh.delete_message_from_federation_queue(
                    fedhash, msg.id, msg.pop_receipt)
        finally:
            self.federations[fedhash].lock.release()

    async def iterate_and_process_federation_queues(
        self
    ) -> Generator[None, None, None]:
        while True:
            if self.federations_available:
                # TODO process in parallel
                for fedhash in self.federations:
                    self.process_federation_queue(fedhash)
            await asyncio.sleep(5)

    async def poll_for_federations(
        self,
        loop: asyncio.BaseEventLoop,
    ) -> Generator[None, None, None]:
        """Poll federations
        :param loop: asyncio loop
        """
        logger.debug('polling federation table {} every {} sec'.format(
            self._service_proxy.table_name_global, self.fed_refresh_interval))
        asyncio.ensure_future(
            self.iterate_and_process_federation_queues(), loop=loop)
        while True:
            self.update_federations()
            await asyncio.sleep(self.fed_refresh_interval)


def main() -> None:
    """Main function"""
    # get command-line args
    args = parseargs()
    # load configuration
    if is_none_or_empty(args.conf):
        raise ValueError('config file not specified')
    with open(args.conf, 'rb') as f:
        config = json.load(f)
    logger.debug('loaded config: {}'.format(config))
    # create federation processor
    fed_processor = FederationProcessor(config)
    # run the poller
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        fed_processor.poll_for_federations(loop)
    )


def parseargs():
    """Parse program arguments
    :rtype: argparse.Namespace
    :return: parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='federation: Azure Batch Shipyard Federation Controller')
    parser.add_argument('--conf', help='configuration file')
    return parser.parse_args()


if __name__ == '__main__':
    _setup_logger()
    main()
