""" This is k2_blockdevice_api docstring """
import logging
import platform
import uuid
import threading
import bitmath
from flocker.node.agents import blockdevice
from zope.interface import implementer
from twisted.python import filepath
from kaminario_flocker_driver.utils.k2_api_client import K2StorageCenterApi, \
    StorageDriverAPIException, InvalidDataException, ImproperConfigurationError
from kaminario_flocker_driver.constants import UNLIMITED_QUOTA, \
    VG_PREFIX, VOL_PREFIX, LEN_OF_DATASET_ID, RETRIES
import eliot

LOG = logging.getLogger(__name__)


class K2BlockDriverLogHandler(logging.Handler):
    """Python log handler to route to Eliot logging."""

    def emit(self, record):
        """Writes log message to the stream.

        :param record: The record to be logged.
        """
        msg = self.format(record)
        eliot.Message.new(
            message_type="flocker:node:agents:blockdevice:k2storagecenter",
            message_level=record.levelname,
            message=msg).write()


def instantiate_driver_instance(cluster_id, **config):
    """Instantiate a new K2 Block device driver instance.

    :param cluster_id: The Flocker cluster ID.
    :param config: The Flocker Driver configuration settings.
    :return: A new StorageCenterBlockDeviceAPI object.
    """
    # Configure log routing to the Flocker Eliot logging
    root_logger = logging.getLogger()
    root_logger.addHandler(K2BlockDriverLogHandler())
    root_logger.setLevel(logging.DEBUG)

    config['cluster_id'] = cluster_id
    return K2BlockDeviceAPI(**config)


@implementer(blockdevice.IBlockDeviceAPI)
class K2BlockDeviceAPI(object):
    """Block device driver for Kaminario (K2) Storage device.
    A "IBlockDeviceAPI" for interacting with Storage Center
    array storage.
    """
    def __init__(self, **kwargs):
        """Initialize new instance of the driver.

        :param config: The driver configuration settings
        such as host, port, username, password.
        :param cluster_id: The cluster ID is running.
        :param is_dedup: The flag to be set for dedup activation.
        :param destroy_host: The flag to set for destroying host if none of the
         volume is mapped
        """
        self.cluster_id = kwargs.get('cluster_id')
        self.instance_name = None
        self.api_client = K2StorageCenterApi(kwargs['storage_host'],
                                             kwargs['username'],
                                             kwargs['password'],
                                             kwargs.get('is_ssl', False),
                                             kwargs.get('retries', RETRIES))
        # Created single instance of krest
        self.krest = self.api_client.connect_to_api()
        self.is_dedup = kwargs.get("is_dedup")
        if self.is_dedup:
            self.is_dedup = self.api_client.is_true(
                self.is_dedup)
        else:
            raise ImproperConfigurationError(
                "'is_dedup' attribute is not set in agent.yml file.")

        self.destroy_host = kwargs.get('destroy_host', False)
        if self.destroy_host:
            self.destroy_host = self.api_client.is_true(
                self.destroy_host)

    def _return_to_block_device_volume(self, volume, attached_to=None):
        """Converts K2 API volume to a `BlockDeviceVolume`.

        With the help of blockdevice_id OS can uniquely identify volume/device
        being referenced by Flocker.
        K2 API returns SCSI Serial number(Page 0x80)(scsi_sn) as
        unique identification of volume.
        """
        dataset_id = uuid.UUID('{00000000-0000-0000-0000-000000000000}')
        try:
            # volume name has a prefix and dataset_id
            # Assumption: dataset id is of 36 chars.
            dataset_id = uuid.UUID("{0}".
                                   format(volume.name)[-LEN_OF_DATASET_ID:])
        except ValueError:
            pass
        ret_val = blockdevice.BlockDeviceVolume(
            blockdevice_id=volume.scsi_sn,
            size=int(self.api_client.kib_to_bytes(volume.size)),
            attached_to=attached_to,
            dataset_id=dataset_id)
        return ret_val

    def _iscsi_rescan(self, process):
        """Performs a SCSI rescan on this host."""
        rescan_thread = threading.Thread(target=self.api_client.rescan_iscsi)
        rescan_thread.name = '{0}_rescan'.format(process)
        rescan_thread.daemon = True
        rescan_thread.start()

    @staticmethod
    def allocation_unit():
        """Gets the minimum allocation unit for our K2 backend.
        The K2 recommended minimum is 1 GiB per volume.
        :returns: 1 GiB in bytes.
        """
        return bitmath.GiB(1).bytes

    def compute_instance_id(self):
        """Gets an identifier for this node.
        This will be compared against ``BlockDeviceVolume.attached_to``
        to determine which volumes are locally attached and it will be used
        with ``attach_volume`` to locally attach volumes.

        For K2, we use the node's hostname as the identifier.

        :returns: A ``unicode`` object giving a provider-specific node
                  identifier which identifies the node where the method
                  is run.
        """
        if not self.instance_name:
            self.instance_name = unicode(platform.uname()[1])
        return self.instance_name

    def _create_new_host(self, attach_to):
        """Create a new host on K2 array

        :param attach_to: It is a hostname of node which is returned
            by "compute_instance_id" method.
        :return: new host
        """
        # Currently host groups not supported.
        # Standalone host creation supported only
        # TODO: This is for host group creation
        # hg_name = u"{}_group".format(attach_to)
        # hg = self.krest.search("host_groups", name=hg_name)
        # if hg.total == 0:
        #     hg = self.krest.new("host_groups", name=hg_name).save()
        # else:
        #     hg = hg.hits[0]
        # host = self.krest.new("hosts", name=attach_to,
        #                 type=self.api_client.get_host_type,
        #                 host_group=hg).save()
        host = self.krest.new("hosts", name=attach_to,
                              type=self.api_client.host_type).save()
        LOG.info("Created new host %s", host)
        return host

    @staticmethod
    def _map_host_with_iqn(iqn_obj, host):
        """ Save or map the host with iqn.

        :param iqn_obj: iqn object from krest
        :param host: host object from krest
        :return: host_iqn object
        """
        host_iqns = iqn_obj
        host_iqns.host = host
        host_iqns.save()
        LOG.info("Saved iqn with host server")
        return host_iqns

    def create_volume(self, dataset_id, size):
        """Create a new volume on the K2 array.

        :param dataset_id: The Flocker dataset ID for the volume.
        :param size: The size of the new volume in bytes.
        :return: A ``BlockDeviceVolume``
        """
        sc_volume = {}
        volume_group = u"{}-{}".format(VG_PREFIX, dataset_id)
        volume_name = u"{}-{}".format(VOL_PREFIX, dataset_id)
        volume_size = self.api_client.bytes_to_kib(size)
        try:
            sc_volume_group = self.krest.new("volume_groups",
                                        name=volume_group,
                                        quota=UNLIMITED_QUOTA,
                                        is_dedup=self.is_dedup).save()
        except Exception as e:
            raise StorageDriverAPIException('Error creating volume group:'
                                            ' {}'.format(e.message))
        if sc_volume_group:
            try:
                sc_volume = self.krest.new("volumes",
                                      name=volume_name,
                                      size=volume_size,
                                      volume_group=sc_volume_group).save()
            except Exception:
                raise StorageDriverAPIException('Error creating volume.')
        return self._return_to_block_device_volume(sc_volume)

    def create_volume_with_profile(self, dataset_id, size, profile_name=None):
        """Create a new volume on the array.

        :param dataset_id: The Flocker dataset ID for the volume.
        :param size: The size of the new volume in bytes.
        :param profile_name: The name of the storage profile for
                             this volume.
        :return: A ``BlockDeviceVolume``
        """
        pass

    def attach_volume(self, blockdevice_id, attach_to):
        """Attach an existing volume to an initiator (host).

        :param blockdevice_id: The unique identifier(scsi_sn of k2)
            for the volume.
        :param attach_to: It is a hostname of node which is returned
            by "compute_instance_id" method.
        :raises UnknownVolume: If the supplied "blockdevice_id" does not
            exist.
        :returns: A  "BlockDeviceVolume" with a "attached_to" attribute set
            to "attach_to".
        """
        LOG.info('attaching to blockdevice_id %s and host is %s',
                 blockdevice_id, attach_to)
        # Searching for volume by scsi_sn via krest
        volume = self.krest.search("volumes", scsi_sn=blockdevice_id)
        if volume.total == 0:
            raise blockdevice.UnknownVolume(blockdevice_id)

        # Check for host which is associate with iqn(iSCSI Qualified Name)
        iqn = self.api_client.get_initiator_name()
        host_iqns = self.krest.search("host_iqns", iqn=iqn)
        host = self.api_client.rgetattr(host_iqns.hits[0], "host", None) \
            if host_iqns.total > 0 else None

        # if iqn is not associate with any host
        if not host:
            # searching instance or node host which is return
            # by compute_instance_id method.
            host = self.krest.search("hosts", name=attach_to)
            if host.total > 0:
                raise InvalidDataException(
                    'Present host is not mapped with iqn')
            else:
                host = self._create_new_host(attach_to)
                self._map_host_with_iqn(host_iqns.hits[0], host)

        # Make sure the server is logged in to the array
        ips = self.krest.search("system/net_ips")
        for ip in ips.hits:
            self.api_client.iscsi_login(
                self.api_client.rgetattr(ip, 'ip_address', None), 3260)

        # Make sure we were able to find host
        if not host:
            raise InvalidDataException('Host does not exits')

        volume = volume.hits[0]
        # First check if we are already mapped
        mapped = self.krest.search('mappings', volume=volume)

        if mapped.total > 0:
            # Get the mapped host
            mapped_host = self.api_client.rgetattr(mapped.hits[0], "host", None)
            if mapped_host != host:
                LOG.info("Mapped server %s", mapped_host)
                # raise exception for attached volume
                raise blockdevice.AlreadyAttachedVolume(blockdevice_id)

        # Make sure host should not be associate with host group
        # Note: Currently host groups not supported.
        try:
            mapping = self.krest.new("mappings", volume=volume, host=host)
            mapping.save()
            LOG.info("Mapping is done- %s", mapping)
        except Exception:
            raise StorageDriverAPIException(
                'Unable to map volume to server.')

        # start iscsi rescan
        self._iscsi_rescan('attach')

        return self._return_to_block_device_volume(volume, attach_to)

    def detach_volume(self, blockdevice_id):
        """Detach ``blockdevice_id`` from whatever host it is attached to.

        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :returns: ``None``
        """
        LOG.info('Detaching %s', blockdevice_id)
        # Check for volume by block device id(scsi_sn)
        volume = self.krest.search("volumes", scsi_sn=blockdevice_id)
        if volume.total == 0:
            raise blockdevice.UnknownVolume(blockdevice_id)

        # First check if we are mapped.
        mapped = self.krest.search("mappings", volume=volume)
        if mapped.total == 0:
            raise blockdevice.UnattachedVolume(blockdevice_id)

        #  executing sync cmd for synchronize data on disk with memory
        self.api_client.sync_device()

        paths = self.api_client.find_paths(blockdevice_id)
        for path in paths:
            if "/dev/mapper/" in path:
                self.api_client.remove_multipath(path)
                break

        # Make sure iqn is mapped with host.
        iqn = self.api_client.get_initiator_name()
        host_iqns = self.krest.search("host_iqns", iqn=iqn)

        if host_iqns.total > 0:
            # find host which is associate with iqn.
            host_iqns = self.api_client.rgetattr(
                host_iqns.hits[0], "host", None)

        # Get the mapped host
        host = self.api_client.rgetattr(mapped.hits[0], "host", None)
        # Make sure host should be exists for volume
        if not host:
            raise StorageDriverAPIException('Unable to locate server.')

        # Make sure both host have same name which is to be unmapped
        if host.name == host_iqns.name:
            mapped.hits[0].delete()
            LOG.info("Removed mapped host %s", host.name)
        if self.destroy_host:
            try:
                host.delete()
            except Exception as e:
                LOG.exception("Unable to delete host due to %s", e.message)
                pass
        # start iscsi rescan
        self._iscsi_rescan('detach')
        return None

    def destroy_volume(self, blockdevice_id):
        """Destroy an existing volume from an initiator (host).

        :param blockdevice_id: The volume unique ID.
        """
        LOG.info('Destroying volume %s', blockdevice_id)
        try:
            volume = self.krest.search("volumes", scsi_sn=blockdevice_id)
            if volume.total == 0:
                raise blockdevice.UnknownVolume(blockdevice_id)
            volume = volume.hits[0]
            volume_group = self.api_client.rgetattr(
                volume, "volume_group", None)
            volume.delete()
            volume_group.delete()
        except Exception:
            raise StorageDriverAPIException(
                'Error destroying volume blockdevice_id:{}'.format(
                    blockdevice_id))
        return None

    def list_volumes(self):
        """List all the block devices available via the back end API.

        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        LOG.info('Listing volumes')
        volumes = []
        vols = self.krest.search("volumes")
        mappings = self.krest.search('mappings')
        # we are removing CTRL volume from volume array, just to pass
        # functional test cases
        # NOTE: CTRL volume is making cause to functional test cases
        for index, vol in enumerate(vols.hits):
            if vol.name == "CTRL":
                del vols.hits[index]
        # Now convert our API objects to Flocker ones
        for vol in vols:
            attached_to = None
            mapped = self.api_client.advance_search(mappings,
                                                    volume__name=vol.name)
            if len(mapped) > 0:
                attached_to = self.api_client.rgetattr(
                    mapped[0], "host.name", None)
            volumes.append(
                self._return_to_block_device_volume(vol, attached_to))
        return volumes

    def get_device_path(self, blockdevice_id):
        """Return the device path.

        Returns the local device path that has been allocated to the block
        device on the host to which it is currently attached.
        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """
        # Check for volume
        volume = self.krest.search("volumes", scsi_sn=blockdevice_id)
        if volume.total == 0:
            raise blockdevice.UnknownVolume(blockdevice_id)

        volume = volume.hits[0]
        # Check for volume is mapped or not
        # NOTE: The assumption right now is if we are mapped,
        # we are mapped to the instance host.
        mapped = self.krest.search("mappings", volume=volume)
        if mapped.total == 0:
            # if not mapped raise exception
            raise blockdevice.UnattachedVolume(blockdevice_id)

        # Get devices path
        paths = self.api_client.find_paths(blockdevice_id)
        if paths:
            # return the first path
            LOG.info('%s path', paths[0])
            return filepath.FilePath(paths[0])
        return None
