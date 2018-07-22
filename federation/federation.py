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
import datetime
import hashlib
import json
import logging
import logging.handlers
import multiprocessing
import pathlib
import pickle
import random
import subprocess
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
import dateutil.tz
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
        '%(asctime)s %(levelname)s %(name)s:%(funcName)s:%(lineno)d '
        '%(message)s')
    formatter.default_msec_format = '%s.%03d'
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def max_workers_for_executor(iterable: Any) -> int:
    """Get max number of workers for executor given an iterable
    :param iterable: an iterable
    :return: number of workers for executor
    """
    return min((len(iterable), _MAX_EXECUTOR_WORKERS))


def is_none_or_empty(obj: Any) -> bool:
    """Determine if object is None or empty
    :param obj: object
    :return: if object is None or empty
    """
    return obj is None or len(obj) == 0


def is_not_empty(obj: Any) -> bool:
    """Determine if object is not None and is length is > 0
    :param obj: object
    :return: if object is not None and length is > 0
    """
    return obj is not None and len(obj) > 0


def datetime_utcnow(as_string: bool=False) -> datetime.datetime:
    """Returns a datetime now with UTC timezone
    :param as_string: return as ISO8601 extended string
    :return: datetime object representing now with UTC timezone
    """
    dt = datetime.datetime.now(dateutil.tz.tzutc())
    if as_string:
        return dt.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
    else:
        return dt


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
        self._config = config
        prefix = config['storage']['entity_prefix']
        self.queue_prefix = '{}fed'.format(prefix)
        self.table_name_global = '{}fedglobal'.format(prefix)
        self.table_name_jobs = '{}fedjobs'.format(prefix)
        self.blob_container_name_global = '{}fedglobal'.format(prefix)
        self.file_share_logging = '{}fedlogs'.format(prefix)
        self._batch_client_lock = threading.Lock()
        self.batch_clients = {}
        # create credentials
        self.creds = Credentials(config)
        # create clients
        self.compute_client = self._create_compute_client()
        self.blob_client = self._create_blob_client()
        self.table_client = self._create_table_client()
        self.queue_client = self._create_queue_client()
        logger.debug('created storage clients for storage account {}'.format(
            self.creds.storage_account))

    @property
    def batch_shipyard_version(self) -> str:
        return self._config['batch_shipyard']['version']

    @property
    def batch_shipyard_var_path(self) -> pathlib.Path:
        return pathlib.Path(self._config['batch_shipyard']['var_path'])

    @property
    def storage_entity_prefix(self) -> str:
        return self._config['storage']['entity_prefix']

    @property
    def logger_level(self) -> str:
        return self._config['logging']['level']

    @property
    def logger_persist(self) -> bool:
        return self._config['logging']['persistence']

    @property
    def logger_filename(self) -> bool:
        return self._config['logging']['filename']

    def log_configuration(self) -> None:
        logger.debug('configuration: {}'.format(
            json.dumps(self._config, sort_keys=True, indent=4)))

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

    def _create_blob_client(self) -> azure.storage.blob.BlockBlobService:
        """Create a blob client for the given storage account
        :return: block blob client
        """
        return azure.storage.blob.BlockBlobService(
            account_name=self.creds.storage_account,
            account_key=self.creds.storage_account_key,
            endpoint_suffix=self.creds.storage_account_ep,
        )

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

    def add_job_schedule(
            self,
            batch_account: str,
            service_url: str,
            jobschedule: batchmodels.JobScheduleAddParameter,
    ) -> None:
        client = self.service_proxy.batch_client(batch_account, service_url)
        client.job_schedule.add(jobschedule)

    def get_job(
            self,
            batch_account: str,
            service_url: str,
            job_id: str,
    ) -> batchmodels.CloudJob:
        client = self.service_proxy.batch_client(batch_account, service_url)
        return client.job.get(job_id)

    def add_job(
            self,
            batch_account: str,
            service_url: str,
            job: batchmodels.JobAddParameter,
    ) -> None:
        client = self.service_proxy.batch_client(batch_account, service_url)
        client.job.add(job)

    async def delete_or_terminate_job(
            self,
            batch_account: str,
            service_url: str,
            job_id: str,
            delete: bool,
            is_job_schedule: bool,
            wait: bool=False,
    ) -> None:
        action = 'delete' if delete else 'terminate'
        cstate = (
            batchmodels.JobScheduleState.completed if is_job_schedule else
            batchmodels.JobState.completed
        )
        client = self.service_proxy.batch_client(batch_account, service_url)
        iface = client.job_schedule if is_job_schedule else client.job
        logger.debug('{} {} {} (account={} service_url={})'.format(
            action, 'job schedule' if is_job_schedule else 'job',
            job_id, batch_account, service_url))
        try:
            if delete:
                iface.delete(job_id)
            else:
                iface.terminate(job_id)
        except batchmodels.batch_error.BatchErrorException as exc:
            if delete:
                if ('does not exist' in exc.message.value or
                        (not wait and
                         'marked for deletion' in exc.message.value)):
                    return
            else:
                if ('completed state' in exc.message.value or
                        'marked for deletion' in exc.message.value):
                    return
        # wait for job to delete/terminate
        if wait:
            while True:
                try:
                    _job = iface.get(job_id)
                    if _job.state == cstate:
                        break
                except batchmodels.batch_error.BatchErrorException as exc:
                    if 'does not exist' in exc.message.value:
                        break
                    else:
                        raise
                await asyncio.sleep(1)

    def _format_generic_task_id(
            self, prefix: str, padding: int, tasknum: int) -> str:
        """Format a generic task id from a task number
        :param prefix: prefix
        :param padding: zfill task number
        :param tasknum: task number
        :return: generic task id
        """
        return '{}{}'.format(prefix, str(tasknum).zfill(padding))

    def regenerate_next_generic_task_id(
            self,
            batch_account: str,
            service_url: str,
            job_id: str,
            naming: Dict[str, Any],
            current_task_id: str,
            last_task_id: Optional[str]=None,
            tasklist: Optional[List[str]]=None,
            is_merge_task: Optional[bool]=False
    ) -> Tuple[List[str], str]:
        """Regenerate the next generic task id
        :param batch_account: batch account
        :param service_url: service url
        :param job_id: job id
        :param naming: naming convention
        :param current_task_id: current task id
        :param tasklist: list of committed and uncommitted tasks in job
        :param is_merge_task: is merge task
        :return: (list of task ids for job, next generic docker task id)
        """
        # get prefix and padding settings
        prefix = naming['prefix']
        if is_merge_task:
            prefix = 'merge-{}'.format(prefix)
        if not current_task_id.startswith(prefix):
            return tasklist, current_task_id
        padding = naming['padding']
        delimiter = prefix if is_not_empty(prefix) else ' '
        client = self.service_proxy.batch_client(batch_account, service_url)
        # get filtered, sorted list of generic docker task ids
        try:
            if tasklist is None:
                tasklist = client.task.list(
                    job_id,
                    task_list_options=batchmodels.TaskListOptions(
                        filter='startswith(id, \'{}\')'.format(prefix)
                        if is_not_empty(prefix) else None,
                        select='id'))
                tasklist = list(tasklist)
            tasknum = sorted(
                [int(x.id.split(delimiter)[-1]) for x in tasklist])[-1] + 1
        except (batchmodels.batch_error.BatchErrorException, IndexError,
                TypeError):
            tasknum = 0
        id = self._format_generic_task_id(prefix, padding, tasknum)
        while id in tasklist:
            try:
                if (last_task_id is not None and
                        last_task_id.startswith(prefix)):
                    tasknum = int(last_task_id.split(delimiter)[-1])
                    last_task_id = None
            except Exception:
                last_task_id = None
            tasknum += 1
            id = self._format_generic_task_id(prefix, padding, tasknum)
        return tasklist, id

    def _submit_task_sub_collection(
            self,
            client: azure.batch.BatchServiceClient,
            job_id: str,
            start: int,
            end: int,
            slice: int,
            all_tasks: List[str],
            task_map: Dict[str, batchmodels.TaskAddParameter]
    ) -> None:
        """Submits a sub-collection of tasks, do not call directly
        :param client: batch client
        :param job_id: job to add to
        :param start: start offset, includsive
        :param end: end offset, exclusive
        :param slice: slice width
        :param all_tasks: list of all task ids
        :param task_map: task collection map to add
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
                results = client.task.add_collection(job_id, chunk)
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
                        if (result.status ==
                                batchmodels.TaskAddStatus.client_error):
                            de = [
                                '{}: {}'.format(x.key, x.value)
                                for x in result.error.values
                            ]
                            logger.error(
                                ('skipping retry of adding task {} as it '
                                 'returned a client error (code={} '
                                 'message={} {}) for job {}').format(
                                     result.task_id, result.error.code,
                                     result.error.message, ' '.join(de),
                                     job_id))
                        elif (result.status ==
                              batchmodels.TaskAddStatus.server_error):
                            retry.append(task_map[result.task_id])
                    if len(retry) > 0:
                        logger.debug(
                            'retrying adding {} tasks to job {}'.format(
                                len(retry), job_id))
                        results = client.task.add_collection(job_id, retry)
                    else:
                        break
            if chunk_end == end:
                break
            start = chunk_end
            slice = initial_slice

    def add_task_collection(
            self,
            batch_account: str,
            service_url: str,
            job_id: str,
            task_map: Dict[str, batchmodels.TaskAddParameter]
    ) -> None:
        """Add a collection of tasks to a job
        :param batch_account: batch account
        :param service_url: service url
        :param job_id: job to add to
        :param task_map: task collection map to add
        """
        client = self.service_proxy.batch_client(batch_account, service_url)
        all_tasks = list(task_map.values())
        if len(all_tasks) == 0:
            logger.debug(
                'no tasks detected in task_map for job {}'.format(job_id))
            return
        slice = 100  # can only submit up to 100 tasks at a time
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=_MAX_EXECUTOR_WORKERS) as executor:
            for start in range(0, len(all_tasks), slice):
                end = start + slice
                if end > len(all_tasks):
                    end = len(all_tasks)
                executor.submit(
                    self._submit_task_sub_collection, client, job_id, start,
                    end, end - start, all_tasks, task_map)
        logger.info('submitted all {} tasks to job {}'.format(
            len(task_map), job_id))

    def set_auto_complete_on_job(
            self,
            batch_account: str,
            service_url: str,
            job_id: str
    ) -> None:
        client = self.service_proxy.batch_client(batch_account, service_url)
        client.job.patch(
            job_id=job_id,
            job_patch_parameter=batchmodels.JobPatchParameter(
                on_all_tasks_complete=batchmodels.
                OnAllTasksComplete.terminate_job
            ),
        )
        logger.debug('set auto-completion for job {}'.format(job_id))


class FederationDataHandler():
    _GLOBAL_LOCK_BLOB = 'global.lock'
    _ALL_FEDERATIONS_PK = '!!FEDERATIONS'
    _FEDERATION_ACTIONS_PREFIX_PK = '!!ACTIONS'
    _MAX_SEQUENCE_ID_PROPERTIES = 10
    _MAX_SEQUENCE_IDS_PER_PROPERTY = 990
    _MAX_STR_ENTITY_PROPERTY_LENGTH = 31744

    def __init__(self, service_proxy: ServiceProxy) -> None:
        """Ctor for Federation data handler
        :param service_proxy: ServiceProxy
        """
        self.service_proxy = service_proxy
        self.lease_id = None

    @property
    def has_global_lock(self) -> bool:
        return self.lease_id is not None

    def lease_global_lock(
        self,
        loop: asyncio.BaseEventLoop,
    ) -> None:
        try:
            if self.lease_id is None:
                logger.debug('acquiring blob lease on {}'.format(
                    self._GLOBAL_LOCK_BLOB))
                self.lease_id = \
                    self.service_proxy.blob_client.acquire_blob_lease(
                        self.service_proxy.blob_container_name_global,
                        self._GLOBAL_LOCK_BLOB, lease_duration=15)
                logger.debug('blob lease acquired on {}'.format(
                    self._GLOBAL_LOCK_BLOB))
            else:
                self.lease_id = \
                    self.service_proxy.blob_client.renew_blob_lease(
                        self.service_proxy.blob_container_name_global,
                        self._GLOBAL_LOCK_BLOB, self.lease_id)
        except Exception:
            self.lease_id = None
        if self.lease_id is None:
            logger.error('could not acquire/renew lease on {}'.format(
                self._GLOBAL_LOCK_BLOB))
        loop.call_later(5, self.lease_global_lock, loop)

    def release_global_lock(self) -> None:
        if self.lease_id is not None:
            try:
                self.service_proxy.blob_client.release_blob_lease(
                    self.service_proxy.blob_container_name_global,
                    self._GLOBAL_LOCK_BLOB, self.lease_id)
            except azure.common.AzureConflictHttpError:
                self.lease_id = None

    def mount_file_storage(self) -> Optional[pathlib.Path]:
        if not self.service_proxy.logger_persist:
            logger.warning('logging persistence is disabled')
            return None
        # create logs directory
        log_path = self.service_proxy.batch_shipyard_var_path / 'logs'
        log_path.mkdir(exist_ok=True)
        # mount
        cmd = (
            'mount -t cifs //{sa}.file.{ep}/{share} {hmp} -o '
            'vers=3.0,username={sa},password={sakey},_netdev,serverino'
        ).format(
            sa=self.service_proxy.creds.storage_account,
            ep=self.service_proxy.creds.storage_account_ep,
            share=self.service_proxy.file_share_logging,
            hmp=log_path,
            sakey=self.service_proxy.creds.storage_account_key,
        )
        logger.debug('attempting to mount file share for logging persistence')
        output = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.PIPE)
        logger.debug(output)
        return log_path

    def unmount_file_storage(self) -> None:
        if not self.service_proxy.logger_persist:
            return
        log_path = self.service_proxy.batch_shipyard_var_path / 'logs'
        cmd = 'umount {hmp}'.format(hmp=log_path)
        logger.debug(
            'attempting to unmount file share for logging persistence')
        output = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.PIPE)
        logger.debug(output)

    def set_log_configuration(self, log_path: pathlib.Path) -> None:
        global logger
        # remove existing handlers
        handlers = logger.handlers[:]
        for handler in handlers:
            handler.close()
            logger.removeHandler(handler)
        # set level
        if self.service_proxy.logger_level == 'info':
            logger.setLevel(logging.INFO)
        elif self.service_proxy.logger_level == 'warning':
            logger.setLevel(logging.WARNING)
        elif self.service_proxy.logger_level == 'error':
            logger.setLevel(logging.ERROR)
        elif self.service_proxy.logger_level == 'critical':
            logger.setLevel(logging.CRITICAL)
        else:
            logger.setLevel(logging.DEBUG)
        # set formatter
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s:%(funcName)s:%(lineno)d '
            '%(message)s')
        formatter.default_msec_format = '%s.%03d'
        # set handlers
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        logger.addHandler(handler_stream)
        # set log file
        if log_path is None:
            logger.warning('not setting logfile as persistence is disabled')
        else:
            logfile = log_path / pathlib.Path(
                self.service_proxy.logger_filename)
            logfile.parent.mkdir(exist_ok=True)
            handler_logfile = logging.handlers.RotatingFileHandler(
                str(logfile), maxBytes=33554432, encoding='utf-8')
            handler_logfile.setFormatter(formatter)
            logger.addHandler(handler_logfile)

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

    def generate_pk_rk_for_job_location_entity(
            self,
            fedhash: str,
            job_id: str,
            pool: 'FederationPool',
    ) -> Tuple[str, str]:
        pk = '{}${}'.format(fedhash, job_id)
        rk = hash_string('{}${}'.format(pool.service_url, pool.pool_id))
        return pk, rk

    def get_location_entity_for_job(
            self,
            fedhash: str,
            job_id: str,
            pool: 'FederationPool',
    ) -> Optional[azure.cosmosdb.table.Entity]:
        pk, rk = self.generate_pk_rk_for_job_location_entity(
            fedhash, job_id, pool)
        try:
            return self.service_proxy.table_client.get_entity(
                self.service_proxy.table_name_jobs, pk, rk)
        except azure.common.AzureMissingResourceHttpError:
            return None

    def location_entities_exist_for_job(
            self,
            fedhash: str,
            job_id: str,
    ) -> bool:
        try:
            entities = self.service_proxy.table_client.query_entities(
                self.service_proxy.table_name_jobs,
                filter='PartitionKey eq \'{}${}\''.format(fedhash, job_id))
            for ent in entities:
                return True
        except azure.common.AzureMissingResourceHttpError:
            pass
        return False

    def upsert_location_entity_for_job(
            self,
            entity: Dict[str, Any],
    ) -> None:
        self.service_proxy.table_client.insert_or_replace_entity(
            self.service_proxy.table_name_jobs, entity)

    def delete_location_entity_for_job(
            self,
            entity: Dict[str, Any],
    ) -> None:
        try:
            self.service_proxy.table_client.delete_entity(
                self.service_proxy.table_name_jobs, entity['PartitionKey'],
                entity['RowKey'])
        except azure.common.AzureMissingResourceHttpError:
            pass

    def get_all_location_entities_for_job(
            self,
            fedhash: str,
            job_id: str,
    ) -> Optional[List[azure.cosmosdb.table.Entity]]:
        try:
            return self.service_proxy.table_client.query_entities(
                self.service_proxy.table_name_jobs,
                filter='PartitionKey eq \'{}${}\''.format(fedhash, job_id)
            )
        except azure.common.AzureMissingResourceHttpError:
            return None

    def delete_action_entity_for_job(
            self,
            entity: Dict[str, Any],
    ) -> None:
        try:
            self.service_proxy.table_client.delete_entity(
                self.service_proxy.table_name_jobs, entity['PartitionKey'],
                entity['RowKey'], if_match=entity['etag'])
        except azure.common.AzureMissingResourceHttpError:
            pass

    def get_messages_from_federation_queue(
            self,
            fedhash: str
    ) -> List[azure.storage.queue.models.QueueMessage]:
        queue_name = '{}-{}'.format(
            self.service_proxy.queue_prefix, fedhash)
        logger.debug('pulling messages from queue {}'.format(queue_name))
        return self.service_proxy.queue_client.get_messages(
            queue_name, num_messages=32, visibility_timeout=1)

    def _get_sequence_entity_for_job(
            self,
            fedhash: str,
            job_id: str
    ) -> azure.cosmosdb.table.Entity:
        return self.service_proxy.table_client.get_entity(
            self.service_proxy.table_name_jobs,
            '{}${}'.format(self._FEDERATION_ACTIONS_PREFIX_PK, fedhash),
            job_id)

    def get_first_sequence_id_for_job(
            self,
            fedhash: str,
            job_id: str
    ) -> str:
        entity = self._get_sequence_entity_for_job(fedhash, job_id)
        try:
            return entity['Sequence0'].split(',')[0]
        except Exception:
            return None

    def pop_and_pack_sequence_ids_for_job(
            self,
            fedhash: str,
            job_id: str,
    ) -> azure.cosmosdb.table.Entity:
        entity = self._get_sequence_entity_for_job(fedhash, job_id)
        seq = []
        for i in range(0, self._MAX_SEQUENCE_ID_PROPERTIES):
            prop = 'Sequence{}'.format(i)
            if prop in entity and is_not_empty(entity[prop]):
                seq.extend(entity[prop].split(','))
        seq.pop(0)
        for i in range(0, self._MAX_SEQUENCE_ID_PROPERTIES):
            prop = 'Sequence{}'.format(i)
            start = i * self._MAX_SEQUENCE_IDS_PER_PROPERTY
            end = start + self._MAX_SEQUENCE_IDS_PER_PROPERTY
            if end > len(seq):
                end = len(seq)
            if start < end:
                entity[prop] = ','.join(seq[start:end])
            else:
                entity[prop] = None
        return entity, len(seq) == 0

    def dequeue_sequence_id_from_federation_sequence(
            self,
            fedhash: str,
            msg_id: str,
            pop_receipt: str,
            target: str,
    ) -> None:
        # pop first item off table sequence
        if is_not_empty(target):
            while True:
                entity, empty_seq = self.pop_and_pack_sequence_ids_for_job(
                    fedhash, target)
                # see if there are no job location entities
                if (empty_seq and not self.location_entities_exist_for_job(
                        fedhash, target)):
                    delete = True
                else:
                    delete = False
                try:
                    if delete:
                        self.delete_action_entity_for_job(entity)
                        logger.debug(
                            'deleted target {} action entity from '
                            'federation {}'.format(target, fedhash))
                    else:
                        etag = self.service_proxy.table_client.\
                            insert_or_replace_entity(
                                self.service_proxy.table_name_jobs, entity)
                        logger.debug(
                            'upserted target {} sequence to federation {} '
                            'etag={}'.format(target, fedhash, etag))
                    break
                except azure.common.AzureConflictHttpError:
                    logger.debug(
                        'conflict upserting target {} sequence to '
                        'federation {}'.format(target, fedhash))
        # dequeue message
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
    def __init__(self, fedhash: str, fedid: str) -> None:
        self.lock = threading.Lock()
        self.hash = fedhash
        self.id = fedid
        self.pools = {}  # type: Dict[str, FederationPool]

    def update_pool(
            self,
            csh: ComputeServiceHandler,
            bsh: BatchServiceHandler,
            entity: azure.cosmosdb.table.Entity,
            poolset: set,
    ) -> str:
        rk = entity['RowKey']
        with self.lock:
            if rk in self.pools:
                poolset.add(rk)
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
        with self.lock:
            poolset.add(rk)
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
            num_tasks: int,
            constraints: Dict[str, Any],
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
        # TODO Parallelize
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

    async def create_job_schedule(
            self,
            bsh: BatchServiceHandler,
            target_pool: str,
            jobschedule: batchmodels.JobScheduleAddParameter,
            constraints: Dict[str, Any],
    ) -> bool:
        """
        This function should be called with lock already held!
        """
        # get pool ref
        pool = self.pools[target_pool]
        # overwrite pool id in job schedule
        jobschedule.job_specification.pool_info.pool_id = pool.pool_id
        # add job schedule
        try:
            logger.info('Adding job schedule {} to pool {}'.format(
                jobschedule.id, pool.pool_id))
            bsh.add_job_schedule(
                pool.batch_account, pool.service_url, jobschedule)
            success = True
        except batchmodels.batch_error.BatchErrorException as exc:
            if 'already exists' not in exc.message.value:
                await bsh.delete_or_terminate_job(
                    pool.batch_account, pool.service_url, jobschedule.id,
                    True, True, wait=True)
            else:
                logger.error(str(exc))
            success = False
        return success

    async def create_job(
            self,
            bsh: BatchServiceHandler,
            target_pool: str,
            job: batchmodels.JobAddParameter,
            constraints: Dict[str, Any],
    ) -> bool:
        """
        This function should be called with lock already held!
        """
        multi_instance = constraints['has_multi_instance']
        auto_complete = constraints['auto_complete']
        # get pool ref
        pool = self.pools[target_pool]
        # overwrite pool id in job
        job.pool_info.pool_id = pool.pool_id
        # add job
        success = False
        del_job = True
        try:
            logger.info('Adding job {} to pool {}'.format(
                job.id, pool.pool_id))
            bsh.add_job(pool.batch_account, pool.service_url, job)
            success = True
            del_job = False
        except batchmodels.batch_error.BatchErrorException as exc:
            if ('The specified job is already in a completed state.' in
                    exc.message.value):
                del_job = False
                logger.error(
                    'cannot reuse completed job {} on pool {}'.format(
                        job.id, pool.pool_id))
            elif 'The specified job already exists' in exc.message.value:
                del_job = False
                success = True
                # cannot re-use an existing job if multi-instance due to
                # job release requirement
                if multi_instance and auto_complete:
                    logger.error(
                        'cannot reuse job {} on pool {} with multi_instance '
                        'and auto_complete'.format(job.id, pool.pool_id))
                    success = False
                else:
                    # retrieve job and check for version consistency
                    _job = bsh.get_job(
                        pool.batch_account, pool.service_url, job.id)
                    # check for task dependencies and job actions
                    # compatibility
                    if (job.uses_task_dependencies and
                            not _job.uses_task_dependencies):
                        logger.error(
                            ('existing job {} on pool {} has an incompatible '
                             'task dependency setting: existing={} '
                             'desired={}').format(
                                 job.id, pool.pool_id,
                                 _job.uses_task_dependencies,
                                 job.uses_task_dependencies))
                        success = False
                    if (_job.on_task_failure != job.on_task_failure):
                        logger.error(
                            ('existing job {} on pool {} has an incompatible '
                             'on_task_failure setting: existing={} '
                             'desired={}').format(
                                 job.id, pool.pool_id,
                                 _job.on_task_failure.value,
                                 job.on_task_failure.value))
                        success = False
            else:
                logger.exception(str(exc))
        if del_job:
            await bsh.delete_or_terminate_job(
                pool.batch_account, pool.service_url, job.id, True, False,
                wait=True)
        return success

    def track_job(
            self,
            fdh: FederationDataHandler,
            target_pool: str,
            job_id: str,
            is_job_schedule: bool,
            unique_id: str,
    ) -> None:
        # get pool ref
        pool = self.pools[target_pool]
        # add to jobs table
        entity = fdh.get_location_entity_for_job(self.hash, job_id, pool)
        if entity is None:
            pk, rk = fdh.generate_pk_rk_for_job_location_entity(
                self.hash, job_id, pool)
            entity = {
                'PartitionKey': pk,
                'RowKey': rk,
                'Kind': 'job_schedule' if is_job_schedule else 'job',
                'PoolId': pool.pool_id,
                'BatchAccount': pool.batch_account,
                'ServiceUrl': pool.service_url,
                'AdditionTimestamps': datetime_utcnow(as_string=True),
                'UniqueIds': unique_id,
            }
        else:
            entity['AdditionTimestamps'] = '{},{}'.format(
                entity['AdditionTimestamps'], datetime_utcnow(as_string=True))
            if (len(entity['AdditionTimestamps']) >
                    self._MAX_STR_ENTITY_PROPERTY_LENGTH):
                tmp = entity['AdditionTimestamps'].split(',')
                entity['AdditionTimestamps'] = ','.join(tmp[-32:])
                del tmp
            entity['UniqueIds'] = '{},{}'.format(
                entity['UniqueIds'], datetime_utcnow(as_string=True))
            if len(entity['UniqueIds']) > self._MAX_STR_ENTITY_PROPERTY_LENGTH:
                tmp = entity['UniqueIds'].split(',')
                entity['UniqueIds'] = ','.join(tmp[-32:])
                del tmp
        logger.debug(
            'upserting location entity for job {} uid {} on pool {} '
            '(batch_account={} service_url={})'.format(
                job_id, unique_id, pool.pool_id, pool.batch_account,
                pool.service_url))
        fdh.upsert_location_entity_for_job(entity)

    def schedule_tasks(
            self,
            bsh: BatchServiceHandler,
            target_pool: str,
            job_id: str,
            constraints: Dict[str, Any],
            naming: Dict[str, Any],
            task_map: Dict[str, batchmodels.TaskAddParameter],
    ) -> None:
        """
        This function should be called with lock already held!
        """
        auto_complete = constraints['auto_complete']
        try:
            merge_task_id = constraints['merge_task']
        except KeyError:
            merge_task_id = None
        # get pool ref
        pool = self.pools[target_pool]
        # re-assign task ids to current job:
        # 1. sort task map keys
        # 2. re-map task ids to current job
        # 3. re-map merge task dependencies
        last_tid = None
        tasklist = None
        task_ids = sorted(task_map.keys())
        for tid in task_ids:
            is_merge_task = tid == merge_task_id
            tasklist, new_tid = bsh.regenerate_next_generic_task_id(
                pool.batch_account, pool.service_url, job_id, naming, tid,
                last_task_id=last_tid, tasklist=tasklist,
                is_merge_task=is_merge_task)
            task = task_map.pop(tid)
            task_map[new_tid] = task
            if is_merge_task:
                merge_task_id = new_tid
            tasklist.append(new_tid)
            last_tid = new_tid
        if merge_task_id is not None:
            merge_task = task_map.pop(merge_task_id)
            merge_task.depends_on = batchmodels.TaskDependencies(
                task_ids=list(task_map.keys()),
            )
            task_map[merge_task_id] = merge_task

        # TODO find orphaned task depends on/ranges - emit warning

        # submit task collection
        bsh.add_task_collection(
            pool.batch_account, pool.service_url, job_id, task_map)
        # set auto complete
        if auto_complete:
            bsh.set_auto_complete_on_job(
                pool.batch_account, pool.service_url, job_id)


class FederationProcessor():
    def __init__(self, config: Dict[str, Any]) -> None:
        """Ctor for FederationProcessor
        :param config: configuration
        """
        self._service_proxy = ServiceProxy(config)
        try:
            self.fed_refresh_interval = int(config['refresh_intervals'].get(
                'federations', 30))
        except KeyError:
            self.fed_refresh_interval = 30
        try:
            self.job_refresh_interval = int(config['refresh_intervals'].get(
                'jobs', 5))
        except KeyError:
            self.job_refresh_interval = 5
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

    def _update_federation(self, entity) -> None:
        fedhash = entity['RowKey']
        fedid = entity['FederationId']
        if fedhash not in self.federations:
            logger.debug('adding federation hash {} id: {}'.format(
                fedhash, fedid))
            self.federations[fedhash] = Federation(fedhash, fedid)
        poolset = set()
        pools = list(self.fdh.get_all_pools_for_federation(fedhash))
        if len(pools) == 0:
            logger.debug('no pools detected for federation {}'.format(fedhash))
            return
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers_for_executor(pools)) as executor:
            for pool in pools:
                executor.submit(
                    self.federations[fedhash].update_pool,
                    self.csh, self.bsh, pool, poolset)
        self.federations[fedhash].trim_orphaned_pools(poolset)

    def update_federations(self) -> None:
        """Update federations"""
        entities = list(self.fdh.get_all_federations())
        if len(entities) == 0:
            return
        logger.debug('starting federations update')
        with self._federation_lock:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers_for_executor(
                        entities)) as executor:
                for entity in entities:
                    executor.submit(self._update_federation, entity)
        logger.debug('completed federation update')

    async def add_job_v1(
            self, fedhash, job, constraints, naming, task_map, unique_id):
        # get the number of tasks in job
        # try to match the appropriate pool for the tasks in job
        # add job to pool
        # if job exists, ensure settings match
        # add tasks to job
        # record mapping in fedjobs table
        num_tasks = len(task_map)
        logger.debug(
            'attempting to match job {} with {} tasks in fed {} '
            'constraints={} naming={}'.format(
                job.id, num_tasks, fedhash, constraints, naming))
        blacklist = set()
        while True:
            # TODO implement graylist and blacklist
            # blacklist = cannot schedule
            # graylist = avoid scheduling
            poolrk = self.federations[fedhash].find_target_pool_for_job(
                self.bsh, num_tasks, constraints, blacklist, unique_id)
            if poolrk is not None:
                cj = await self.federations[fedhash].create_job(
                    self.bsh, poolrk, job, constraints)
                if cj:
                    self.federations[fedhash].track_job(
                        self.fdh, poolrk, job.id, False, unique_id)
                    self.federations[fedhash].schedule_tasks(
                        self.bsh, poolrk, job.id, constraints, naming,
                        task_map)
                    break
                else:
                    blacklist.add(poolrk)
            else:
                await asyncio.sleep(1 + random.randint(0, 5))

    async def add_job_schedule_v1(
            self, fedhash, job_schedule, constraints, unique_id):
        # ensure there is no existing job schedule. although this is checked
        # at submission time, a similarly named job schedule can be enqueued
        # multiple times before the action is dequeued
        if self.fdh.location_entities_exist_for_job(fedhash, job_schedule.id):
            logger.error(
                'job schedule {} already exists for fed {} uid={}'.format(
                    job_schedule.id, fedhash, unique_id))
            return
        num_tasks = constraints['tasks_per_recurrence']
        logger.debug(
            'attempting to match job schedule {} with {} tasks in fed {} '
            'constraints={}'.format(
                job_schedule.id, num_tasks, fedhash, constraints))
        blacklist = set()
        while True:
            # TODO implement graylist and blacklist
            # blacklist = cannot schedule
            # graylist = avoid scheduling
            poolrk = self.federations[fedhash].find_target_pool_for_job(
                self.bsh, num_tasks, constraints, blacklist, unique_id)
            if poolrk is not None:
                cj = await self.federations[fedhash].create_job_schedule(
                    self.bsh, poolrk, job_schedule, constraints)
                if cj:
                    self.federations[fedhash].track_job(
                        self.fdh, poolrk, job_schedule.id, True, unique_id)
                    break
                else:
                    blacklist.add(poolrk)
            else:
                await asyncio.sleep(1 + random.randint(0, 5))

    async def _terminate_job(self, fedhash, job_id, is_job_schedule, entity):
        if 'TerminateTimestamp' in entity:
            logger.debug(
                '{} {} for fed {} has already been terminated '
                'at {}'.format(
                    'job schedule' if is_job_schedule else 'job',
                    job_id, fedhash, entity['TerminateTimestamp']))
            return
        logger.debug(
            'terminating {} {} on pool {} for fed {} (batch_account={} '
            'service_url={}'.format(
                'job schedule' if is_job_schedule else 'job',
                job_id, entity['PoolId'], fedhash, entity['BatchAccount'],
                entity['ServiceUrl']))
        await self.bsh.delete_or_terminate_job(
            entity['BatchAccount'], entity['ServiceUrl'], job_id, False,
            is_job_schedule, wait=False)
        entity['TerminateTimestamp'] = datetime_utcnow(as_string=False)
        self.fdh.upsert_location_entity_for_job(entity)

    async def _delete_job(self, fedhash, job_id, is_job_schedule, entity):
        logger.debug(
            'deleting {} {} on pool {} for fed {} (batch_account={} '
            'service_url={}'.format(
                'job schedule' if is_job_schedule else 'job',
                job_id, entity['PoolId'], fedhash, entity['BatchAccount'],
                entity['ServiceUrl']))
        await self.bsh.delete_or_terminate_job(
            entity['BatchAccount'], entity['ServiceUrl'], job_id, True,
            is_job_schedule, wait=False)
        self.fdh.delete_location_entity_for_job(entity)

    async def delete_or_terminate_job_v1(
            self, delete, fedhash, job_id, is_job_schedule, unique_id):
        # find all jobs across federation mathching the id
        entities = self.fdh.get_all_location_entities_for_job(fedhash, job_id)
        # terminate each pool-level job representing federation job
        tasks = []
        coro = self._delete_job if delete else self._terminate_job
        for entity in entities:
            tasks.append(
                asyncio.ensure_future(
                    coro(fedhash, job_id, is_job_schedule, entity)))
        if len(tasks) > 0:
            await asyncio.wait(tasks)
        else:
            logger.warning(
                'cannot {} {} {} for fed {}, no location entities '
                'exist (uid={})'.format(
                    'delete' if delete else 'terminate',
                    'job schedule' if is_job_schedule else 'job',
                    job_id, fedhash, unique_id))
            return

    async def process_message_action_v1(
            self, fedhash, action, target_type, job_id, data, unique_id):
        if is_not_empty(data) and data['version'] != '1':
            logger.error('cannot process job data version {} for {}'.format(
                data['version'], unique_id))
            return
        if target_type == 'job_schedule':
            if action == 'add':
                if job_id != data[target_type]['id']:
                    logger.error(
                        'job schedule id mismatch q:{} != b:{} for '
                        'fed {}'.format(
                            job_id, data[target_type]['id'], fedhash))
                    return
                job_schedule = data[target_type]['data']
                constraints = data[target_type]['constraints']
                await self.add_job_schedule_v1(
                    fedhash, job_schedule, constraints, unique_id)
            elif action == 'terminate':
                await self.delete_or_terminate_job_v1(
                    False, fedhash, job_id, True, unique_id)
            elif action == 'delete':
                await self.delete_or_terminate_job_v1(
                    True, fedhash, job_id, True, unique_id)
            else:
                raise NotImplementedError()
        elif target_type == 'job':
            if action == 'add':
                if job_id != data[target_type]['id']:
                    logger.error(
                        'job id mismatch q:{} != b:{} for fed {}'.format(
                            job_id, data[target_type]['id'], fedhash))
                    return
                job = data[target_type]['data']
                constraints = data[target_type]['constraints']
                naming = data[target_type]['naming']
                task_map = data['task_map']
                await self.add_job_v1(
                    fedhash, job, constraints, naming, task_map, unique_id)
            elif action == 'terminate':
                await self.delete_or_terminate_job_v1(
                    False, fedhash, job_id, False, unique_id)
            elif action == 'delete':
                await self.delete_or_terminate_job_v1(
                    True, fedhash, job_id, False, unique_id)
            else:
                raise NotImplementedError()
        else:
            logger.error('unknown target type: {}'.format(target_type))

    async def process_queue_message_v1(
            self,
            fedhash: str,
            msg: Dict[str, Any]
    ) -> Tuple[bool, str]:
        target_fedid = msg['federation_id']
        calc_fedhash = hash_federation_id(target_fedid)
        if calc_fedhash != fedhash:
            logger.error(
                'federation hash mismatch, expected={} actual={} id={}'.format(
                    fedhash, calc_fedhash, target_fedid))
            return True, None
        action = msg['action']['method']
        target_type = msg['action']['kind']
        unique_id = msg[target_type]['uuid']
        job_id = msg[target_type]['id']
        # get sequence from table
        seq_id = self.fdh.get_first_sequence_id_for_job(fedhash, job_id)
        if seq_id is None:
            logger.error(
                'sequence length is non-positive for job {} on '
                'federation {}'.format(job_id, fedhash))
            return True, None

        # TODO warn only, then process first seq_id instead of unique_id
        # create method to construct blob_data
        # check method is add and kind
        # modify retrieve_blob_data to understand non-sas and
        # fallback to blob client on federation to download

        if seq_id != unique_id:
            logger.warning(
                'skipping queue message for fed {} that does not match first '
                'sequence q:{} != t:{} for job {}'.format(
                    fedhash, unique_id, seq_id, job_id))
            return False, None

        job_data = None
        # retrieve job/task data
        if 'blob_data' in msg[target_type]:
            blob_client, container, blob_name, data = \
                self.fdh.retrieve_blob_data(msg[target_type]['blob_data'])
            job_data = pickle.loads(data, fix_imports=True)
            del data
        # process message
        await self.process_message_action_v1(
            fedhash, action, target_type, job_id, job_data, unique_id)
        # delete blob
        if job_data is not None:
            self.fdh.delete_blob(blob_client, container, blob_name)
        return True, job_id

    async def process_federation_queue(self, fedhash: str) -> None:
        acquired = self.federations[fedhash].lock.acquire(blocking=False)
        if not acquired:
            logger.debug('could not acquire lock on federation {}'.format(
                fedhash))
            return
        try:
            msgs = self.fdh.get_messages_from_federation_queue(fedhash)
            for msg in msgs:
                if not await self.check_global_lock(backoff=False):
                    return
                msg_data = json.loads(msg.content, encoding='utf8')
                if msg_data['version'] == '1':
                    del_msg, target = await self.process_queue_message_v1(
                        fedhash, msg_data)
                else:
                    logger.error(
                        'cannot process message version {} for fed {}'.format(
                            msg_data['version'], fedhash))
                    del_msg = True
                    target = None
                # delete message
                if del_msg:
                    self.fdh.dequeue_sequence_id_from_federation_sequence(
                        fedhash, msg.id, msg.pop_receipt, target)
        finally:
            self.federations[fedhash].lock.release()

    async def check_global_lock(
        self,
        backoff: bool=True
    ) -> Generator[None, None, None]:
        if not self.fdh.has_global_lock:
            if backoff:
                await asyncio.sleep(5 + random.randint(0, 5))
            return False
        return True

    async def iterate_and_process_federation_queues(
        self
    ) -> Generator[None, None, None]:
        while True:
            if not await self.check_global_lock():
                continue
            if self.federations_available:
                # TODO process in parallel
                for fedhash in self.federations:
                    await self.process_federation_queue(fedhash)
            await asyncio.sleep(self.job_refresh_interval)

    async def poll_for_federations(
        self,
        loop: asyncio.BaseEventLoop,
    ) -> Generator[None, None, None]:
        """Poll federations
        :param loop: asyncio loop
        """
        # lease global lock blob
        self.fdh.lease_global_lock(loop)
        # mount log storage
        log_path = self.fdh.mount_file_storage()
        # set logging configuration
        self.fdh.set_log_configuration(log_path)
        self._service_proxy.log_configuration()
        logger.debug('polling federation table {} every {} sec'.format(
            self._service_proxy.table_name_global, self.fed_refresh_interval))
        logger.debug('polling jobs table {} every {} sec'.format(
            self._service_proxy.table_name_jobs, self.job_refresh_interval))
        # begin message processing
        asyncio.ensure_future(
            self.iterate_and_process_federation_queues(), loop=loop)
        # continuously update federations
        while True:
            if not await self.check_global_lock():
                continue
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
    logger.debug('loaded config from {}: {}'.format(args.conf, config))
    # create federation processor
    fed_processor = FederationProcessor(config)
    # run the poller
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            fed_processor.poll_for_federations(loop)
        )
    except Exception as exc:
        logger.exception(str(exc))
    finally:
        handlers = logger.handlers[:]
        for handler in handlers:
            handler.close()
            logger.removeHandler(handler)
        try:
            fed_processor.fdh.unmount_file_storage()
        except Exception as exc:
            logger.exception(str(exc))


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
