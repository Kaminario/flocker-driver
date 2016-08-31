""" This is iscsi_utils docstring """
from datetime import datetime
import logging
import os
import re
import shlex
import time
from subprocess import CalledProcessError, check_output
from kaminario_flocker_driver.constants import DELAY, ITERATION_LIMIT, \
    RESCAN_DELAY


LOG = logging.getLogger(__name__)


class IscsiUtils(object):
    """iSCSI utilities for smooth communication of Host Server with K2 array"""

    @staticmethod
    def _run_command(cmd):
        """
           Run a command and capture its output. Used for common code in
           the implementation of many methods of the iscsi interface.
        :param cmd: The command arguments(the first argument
                    for the check_output)
        :returns: The output captured from the execution of the command.
        """
        status = 0
        try:
            LOG.info('Running %s', cmd)
            output = check_output(shlex.split(cmd))
            if output:
                LOG.debug('Result: %s', output)
        except CalledProcessError as call_error:
            output = ""
            status = call_error.message
        except OSError as os_err:
            output = ""
            status = os_err.message

        return output, status

    def _iscsi_login_logout(self, target_iqn, login_action):
        """
            Perform the iSCSI login or logout depending on the caller
            e.g. iscsiadm -m node -T
                 iqn.2009-01.com.kaminario:storage.k2.54615 --login
        """
        try:
            action = "-u"
            if login_action:
                action = "-l"

            output, status = self._run_command(
                'iscsiadm -m node -T {} {}'.format(target_iqn, action))
            if status == 0:
                LOG.info('Performed %s to %s', action, target_iqn)
                return True
        except CalledProcessError:
            LOG.info('Error logging in.')
        return False

    def _iscsi_discovery_login_logout(self, ip_address,
                                      port, login_action=True):
        """Manage iSCSI sessions for K2 storage device data ports."""
        discovery_output, status = self._run_command(
            'iscsiadm -m discovery -t st -p {}'.format(ip_address))
        lines = discovery_output.split('\n')
        for line in lines:
            if ':' not in line:
                continue
            target = line.split(' ')
            target_iqn = target[1]
            self._iscsi_login_logout(target_iqn, login_action)
        return None

    def _get_multipath_device(self, scsi_device):
        """
        Get the multi-path device for a K2 volume.

        Output from `multi-path -ll` should be something like:
        mpathb (20024f400d5570001) dm-2 KMNRIO ,k2
        size=2.0G features='0' hwhandler='0' wp=rw
        `-+- policy='queue-length 0' prio=1 status=active
          |- 11:0:0:2 sdw 8:32 active ready running
          |- 12:0:0:2 sdx 8:64 active ready running
          |- 13:0:0:2 sdy 8:112 active ready running
          `- 14:0:0:2 sdz 8:128 active ready running

        :param scsi_device: The SCSI device to look for.
        :return: The path(e.g. /dev/mapper/mpathbd) for
        multipath device if one exists.
        """
        result = None
        try:
            output, status = self._run_command(
                'multipath -l {}'.format(scsi_device))
            if output:
                lines = output.split('\n')
                for line in lines:
                    if 'KMNRIO' not in line:
                        continue
                    name = line.split(' ')[0]
                    result = '/dev/mapper/{}'.format(name)
                    break
        except Exception as e:
            LOG.error("Multipath error: %s", e.message)
        return result

    def iscsi_login(self, ip_address, port=3260):
        """Perform an iSCSI login into K2 device.
        :param ip_address: system ip
        :param port: system port number
        :return None
        """
        return self._iscsi_discovery_login_logout(ip_address, port, True)

    def iscsi_logout(self, ip_address, port=3260):
        """Perform an iSCSI logout from K2 device.
        :param ip_address: system ip
        :param port: system port number
        :return None
        """
        return self._iscsi_discovery_login_logout(ip_address, port, False)

    def get_initiator_name(self):
        """Gets the iSCSI initiator name for current Host server/VM."""
        initiator_name, status = self._run_command(
            'cat /etc/iscsi/initiatorname.iscsi')

        if initiator_name:
            lines = initiator_name.split('\n')
            for line in lines:
                if '=' in line:
                    parts = line.split('=')
                    return parts[1]
        else:
            LOG.info('There is no iscsi initiator-name for this host server.')
            return None

    def _rescan_iscsi_session(self):
        """Perform an iSCSI session rescan."""
        start = datetime.now()
        output, status = self._run_command('iscsiadm -m session --rescan')
        lines = output.split('\n')
        end = datetime.now()
        LOG.info('Rescan iscsi session %s --output: %s', (end - start), lines)
        return None

    def _run_scsi_bus(self):
        """Executing rescan-scsi-bus.sh
        Discovers the Luns
        :return: None
        """
        output, status = self._run_command('rescan-scsi-bus.sh')
        lines = output.split('\n')
        LOG.info('Executing scsi bus --output %s', lines)
        return None

    def _run_multipath(self):
        """Executing multipath command to update multipath info
        :return: None
        """
        self._run_command('multipath')
        LOG.info('Executed multipath')
        return None

    def rescan_iscsi(self):
        """Rescan iSCSI Device
        :return: None
        """
        # Delay is added, to make sure that host should enough time to retrieve
        # multipath device
        time.sleep(RESCAN_DELAY)
        self._rescan_iscsi_session()  # Rescan iSCSI session
        time.sleep(RESCAN_DELAY)
        self._run_scsi_bus()  # Rescan scsi bus
        time.sleep(RESCAN_DELAY)
        self._run_multipath()
        LOG.info('iSCSI rescan successfully completed.')
        return None

    def find_paths(self, device_id):
        """
            Looks for the local/physical device paths.
            Note: The first element will be the multipath device
                  if one is present.
            TODO: Add comments for multipath device.
        :param device_id: The page 80 device id.
        :returns: A list of the local paths.
        """
        result = []
        regex = re.compile(r'sd[a-z]+(?![\d])')
        for dev in os.listdir('/dev/'):
            if regex.match(dev):
                try:
                    output, status = self._run_command(
                        '/lib/udev/scsi_id --page=0x80 '
                        '--whitelisted --device=/dev/{}'.format(dev))
                    if device_id in output:
                        LOG.info('Found %s at %s', device_id, dev)
                        result.append('/dev/{}'.format(dev))
                except Exception:
                    LOG.exception('Error while getting device id for %s', dev)

        # Functional tests always want the same device reported
        result.sort()

        if result:
            # Check if there is a multipath device and make sure it should
            # return multipath, we can refresh multipath discovery
            # via iterating in loop which is controlled by ITERATION_LIMIT
            retries = 0
            while retries < ITERATION_LIMIT:
                mpath_dev = self._get_multipath_device(result[0])
                if mpath_dev:
                    LOG.info('Found multipath device %s', mpath_dev)
                    result.insert(0, mpath_dev)
                    break
                retries += 1
                time.sleep(DELAY)
        return result

    def sync_device(self):
        """synchronize data on disk with memory

        sync writes any data buffered in memory out to disk. This can include
        (but is not limited to) modified superblocks, modified inodes,
        and delayed reads and writes.
        :return:
        """
        self._run_command('sync')
        LOG.info('Executed sync')
        return None

    def remove_multipath(self, mpath):
        """
        Performing removal of multipath device
        :param mpath: path(e.g. /dev/mapper/X) for Multipath device
        :return: None
        """
        if not mpath:
            return None
        if '/dev/mapper' in mpath:
            try:
                path = mpath.replace('/dev/mapper/', '')
                self._run_command('multipath -f {}'.format(path))
            except Exception:
                LOG.exception('Error removing multipath device %s', mpath)
        return None
