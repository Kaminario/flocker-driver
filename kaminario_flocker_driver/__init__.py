""" This is KAMINARIO-FLOCKER-DRIVER Module docstring """
from flocker import node
from kaminario_flocker_driver.k2_blockdevice_api \
    import instantiate_driver_instance
from kaminario_flocker_driver.constants import DRIVER_NAME


def api_factory(cluster_id, **kwargs):
    """Entry point for Flocker to load driver instance."""
    kwargs['cluster_id'] = cluster_id
    return instantiate_driver_instance(
        **kwargs)

FLOCKER_BACKEND = node.BackendDescription(
    name=DRIVER_NAME,
    needs_reactor=False,
    needs_cluster_id=True,
    api_factory=api_factory,
    deployer_type=node.DeployerType.block)
