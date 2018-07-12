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
import hashlib
import json
import logging
import logging.handlers
import pathlib
import threading
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
)
# non-stdlib imports
import azure.batch
import azure.batch.models as batchmodels
import azure.cosmosdb.table
import azure.mgmt.compute
import azure.mgmt.resource
import azure.mgmt.storage
import azure.storage.queue
import msrestazure.azure_active_directory
import msrestazure.azure_cloud
import requests

# create logger
logger = logging.getLogger(__name__)
# global defines
_VALID_NODE_STATES = frozenset((
    batchmodels.ComputeNodeState.idle,
    batchmodels.ComputeNodeState.offline,
    batchmodels.ComputeNodeState.running,
    batchmodels.ComputeNodeState.starting,
    batchmodels.ComputeNodeState.start_task_failed,
    batchmodels.ComputeNodeState.waiting_for_start_task,
))


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


def is_none_or_empty(obj):
    # type: (any) -> bool
    """Determine if object is None or empty
    :type any obj: object
    :rtype: bool
    :return: if object is None or empty
    """
    return obj is None or len(obj) == 0


def is_not_empty(obj):
    # type: (any) -> bool
    """Determine if object is not None and is length is > 0
    :type any obj: object
    :rtype: bool
    :return: if object is not None and length is > 0
    """
    return obj is not None and len(obj) > 0


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

    def get_batch_pool_full_update(
            self,
            batch_account: str,
            service_url: str,
            poolid: str,
    ) -> batchmodels.CloudPool:
        client = self.service_proxy.batch_client(batch_account, service_url)
        try:
            return client.pool.get(poolid)
        except batchmodels.BatchErrorException as e:
            logger.error(
                'could not retrieve Batch pool {} (account={} '
                'service_url={})'.format(poolid, batch_account, service_url))


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
        pass



# def _construct_batch_monitoring_list(
#         batch_client: azure.batch.BatchServiceClient,
#         poolid: str
# ) -> List[Dict]:
#     """Construct the batch pool monitoring list
#     :param batch_client: batch client
#     :param poolid: pool id
#     """
#     logger.debug('querying batch pool: {}'.format(poolid))
#     # first retrieve node exporter and cadvisor ports
#     try:
#         pool = batch_client.pool.get(
#             poolid,
#             pool_get_options=batchmodels.PoolGetOptions(
#                 select='id,state,startTask',
#             ),
#         )
#     except batchmodels.BatchErrorException as e:
#         logger.error(e.message)
#         return None
#     if pool.state == batchmodels.PoolState.deleting:
#         logger.debug('pool {} is being deleted, ignoring'.format(pool.id))
#         return None
#     ne_port = None
#     ca_port = None
#     for es in pool.start_task.environment_settings:
#         if es.name == 'PROM_NODE_EXPORTER_PORT':
#             ne_port = int(es.value)
#         elif es.name == 'PROM_CADVISOR_PORT':
#             ca_port = int(es.value)
#         if ne_port is not None and ca_port is not None:
#             break
#     logger.debug('pool {} state={} ne_port={} ca_port={}'.format(
#         pool.id, pool.state, ne_port, ca_port))
#     # get node list
#     nodelist = []
#     try:
#         nodes = batch_client.compute_node.list(
#             poolid,
#             compute_node_list_options=batchmodels.ComputeNodeListOptions(
#                 select='id,state,ipAddress',
#             ),
#         )
#         for node in nodes:
#             logger.debug('compute node {} state={} ipaddress={}'.format(
#                 node.id, node.state.value, node.ip_address))
#             if node.state in _VALID_NODE_STATES:
#                 nodelist.append(node)
#     except batchmodels.BatchErrorException as e:
#         logger.error(e.message)
#         return None
#     if is_none_or_empty(nodelist):
#         logger.info('no viable nodes found in pool: {}'.format(poolid))
#         return None
#     logger.info('monitoring {} nodes for pool: {}'.format(
#         len(nodelist), poolid))
#     # construct prometheus targets
#     targets = []
#     if ne_port is not None:
#         targets.append(
#             {
#                 'targets': [
#                     '{}:{}'.format(x.ip_address, ne_port) for x in nodelist
#                 ],
#                 'labels': {
#                     'env': _MONITOR_BATCHPOOL_PK,
#                     'collector': 'NodeExporter',
#                     'job': '{}'.format(poolid)
#                 }
#             }
#         )
#     if ca_port is not None:
#         targets.append(
#             {
#                 'targets': [
#                     '{}:{}'.format(x.ip_address, ca_port) for x in nodelist
#                 ],
#                 'labels': {
#                     'env': _MONITOR_BATCHPOOL_PK,
#                     'collector': 'cAdvisor',
#                     'job': '{}'.format(poolid)
#                 }
#             }
#         )
#     return targets


class FederationPool():
    def __init__(
            self,
            batch_account: str,
            service_url: str,
            location: str,
            cloud_pool: batchmodels.CloudPool,
            vm_size: 'azure.mgmt.compute.models.VirtualMachineSize'
    ) -> None:
        self.batch_account = batch_account
        self.service_url = service_url
        self.location = location
        self.cloud_pool = cloud_pool
        self.vm_props = vm_size
        self._vm_size = None
        if self.is_valid:
            self._vm_size = self.cloud_pool.vm_size.lower()

    @property
    def is_valid(self) -> bool:
        return self.cloud_pool is not None and self.vm_props is not None

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
        self.hash = fedhash
        self.id = fedid
        self.pools = {}

    def update_pool(
            self,
            csh: ComputeServiceHandler,
            bsh: BatchServiceHandler,
            entity: azure.cosmosdb.table.Entity
    ) -> str:
        rk = entity['RowKey']
        if rk in self.pools:
            return rk
        batch_account = entity['BatchAccount']
        poolid = entity['PoolId']
        service_url = entity['BatchServiceUrl']
        location = entity['Location']
        csh.populate_vm_sizes_from_location(location)
        pool = bsh.get_batch_pool_full_update(
            batch_account, service_url, poolid)
        vm_size = None
        if pool is not None:
            vm_size = csh.get_vm_size(pool.vm_size)
        fedpool = FederationPool(
            batch_account, service_url, location, pool, vm_size
        )
        self.pools[rk] = fedpool
        if self.pools[rk].is_valid:
            logger.info(
                'added valid pool {} id={} to federation {} id={} for '
                'account {} at location {} size={} ppn={} mem={}'.format(
                    rk, poolid, self.hash, self.id, batch_account, location,
                    fedpool.vm_size, fedpool.vm_props.number_of_cores,
                    fedpool.vm_props.memory_in_mb))
        else:
            logger.info(
                'added invalid pool {} id={} to federation {} id={}'.format(
                    rk, poolid, self.hash, self.id))
        return rk

    def trim_orphaned_pools(self, fedpools: set) -> None:
        # do not get symmetric difference
        diff = [x for x in self.pools.keys() if x not in fedpools]
        for rk in diff:
            logger.debug('removing pool {} from federation {} id={}'.format(
                rk, self.hash, self.id))
            self.pools.pop(rk)
        logger.info('active pools in federation {} id={}: {}'.format(
            self.hash, self.id, ' '.join(self.pools.keys())))


class FederationProcessor():
    def __init__(self, config: Dict[str, Any]) -> None:
        """Ctor for FederationProcessor
        :param config: configuration
        """
        self._service_proxy = ServiceProxy(config)
        self.fed_refresh_interval = config.get(
            'federation_refresh_interval', 15)
        self.csh = ComputeServiceHandler(self._service_proxy)
        self.bsh = BatchServiceHandler(self._service_proxy)
        self.fdh = FederationDataHandler(self._service_proxy)
        # data structs
        self._federation_lock = threading.Lock()
        self.federations = {}

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

    async def process_federation_queue(self, fedhash):
        logger.debug(
            'starting federation {} queue processing'.format(fedhash))


        logger.debug(
            'completed federation {} queue processing'.format(fedhash))

    async def iterate_and_process_federation_queues(self):
        while True:
            if self.federations_available:
                # TODO process in parallel
                for fedhash in self.federations:
                    await self.process_federation_queue(fedhash)
            await asyncio.sleep(2)

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
