#    Kaminario Block Device Driver for ClusterHQ/Flocker
"""
Functional tests for
``flocker.node.agents.blockdevice.K2BlockDeviceAPI``
"""
import logging
import os
from uuid import uuid4
import yaml
import bitmath
from flocker.node.agents import blockdevice
from flocker.node.agents.test.test_blockdevice import (
    make_iblockdeviceapi_tests)
from twisted.python.components import proxyForInterface
from zope.interface import implementer
from kaminario_flocker_driver.k2_blockdevice_api import (
    instantiate_driver_instance)

MIN_ALLOCATION_SIZE = bitmath.GiB(1).bytes
MIN_ALLOCATION_UNIT = MIN_ALLOCATION_SIZE

LOG = logging.getLogger(__name__)


@implementer(blockdevice.IBlockDeviceAPI)
class TestK2FlockerDriver(proxyForInterface(blockdevice.IBlockDeviceAPI,
                                            'original')):
    """Wrapper around driver class to provide test cleanup."""
    def __init__(self, original):
        self.original = original
        self.volumes = {}

    def _cleanup(self):
        """Clean up testing environment."""
        print "Enter into Clean up"
        for vol in self.volumes:
            # Makes sure all the created test Volumes are destroyed
            try:
                self.original.detach_volume(self.volumes[vol])
            except Exception:
                LOG.exception('Error detaching the test volume.')

            try:
                self.original.destroy_volume(self.volumes[vol])
            except Exception:
                LOG.exception('Error cleaning up the test volume.')

    def create_volume(self, dataset_id, size):
        """Track all volume creation."""
        print "Testing create volume"
        blockdevvol = self.original.create_volume(dataset_id, size)
        self.volumes[u"{}".format(dataset_id)] = blockdevvol.blockdevice_id
        print self.volumes, "volumes"
        return blockdevvol


def api_factory(test_case):
    """Create a test instance of the block driver.

    :param test_case: The specific test case instance.
    :return: A test configured driver instance.
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-7s [%(threadName)-19s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        filename='../k2_driver.log')
    test_config_path = os.environ.get(
        'FLOCKER_CONFIG',
        '../k2f_agent.yml')
    if not os.path.exists(test_config_path):
        raise Exception('Functional test configuration not found.')

    with open(test_config_path) as config_file:
        config = yaml.load(config_file.read())

    config = config.get('dataset', {})
    test_driver = TestK2FlockerDriver(
        instantiate_driver_instance(
            cluster_id=uuid4(),
            **config))
    test_case.addCleanup(test_driver._cleanup)
    return test_driver


class K2BlockDeviceAPITests(make_iblockdeviceapi_tests(
        blockdevice_api_factory=(lambda test_case: api_factory(test_case)),
        minimum_allocatable_size=MIN_ALLOCATION_SIZE,
        device_allocation_unit=MIN_ALLOCATION_UNIT,
        unknown_blockdevice_id_factory=lambda test: unicode(uuid4()))):
    """ Functional test cases for Kaminario Flocker driver"""
    pass

# To run functional test cases for Kaminario Flocker driver,
# please uncomment the following two lines of code
# suite = unittest.TestLoader().loadTestsFromTestCase(K2BlockDeviceAPITests)
# unittest.TextTestRunner(verbosity=3).run(suite)
