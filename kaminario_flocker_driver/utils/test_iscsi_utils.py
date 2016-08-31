""" This Unit Test code for iscsi_utils """

import unittest
from kaminario_flocker_driver.utils.iscsi_utils import IscsiUtils


class IscsiUtilsTest(unittest.TestCase):
    """Tests for `iscsi_utils.py`."""
    iscsi_obj = IscsiUtils()

    def test_run_command_none(self):
        """Is "None" command returns successfully?"""
        output, status = self.iscsi_obj._run_command("None")
        self.assertIs(output, "", "None Command Failed")

    def test_run_command_not_none(self):
        """Is "ls" command returns successfully?"""
        self.assertIsNotNone(self.iscsi_obj._run_command("ls -lrt"),
                             "'ls -lrt' command Failed")

    def test_run_command_equal(self):
        """Is "sync" command returns successfully?"""
        output, status = self.iscsi_obj._run_command("sync")
        self.assertEqual(output, "", "'sync' command Failed")

    def test_iscsi_login_logout_login_true(self):
        """Is proper iscsi login command returns successfully?"""
        self.assertEqual(self.iscsi_obj._iscsi_login_logout(
            "iqn.2009-01.com.kaminario:storage.k2.54615", "-l"), True,
                         "K2 LOGIN Failed")

    def test_iscsi_login_logout_logout_false(self):
        """Is wrong IQN returns successfully?"""
        self.assertFalse(self.iscsi_obj._iscsi_login_logout(
            "iqn.2009-01.com.kaminario:storage.k2.testing", "-u"),
                         "Incorrect IQN")

    def test_iscsi_login_logout_iqn_false(self):
        """Is wrong IQN and Login returns successfully?"""
        self.assertFalse(
            self.iscsi_obj._iscsi_login_logout("iqn.testing", "login"),
            "Incorrect IQN/command")

    def test_iscsi_discovery_login_logout_success(self):
        """Is proper iscsi discovery command returns successfully?"""
        self.assertIsNone(self.iscsi_obj._iscsi_discovery_login_logout(
            "10.11.57.2", "3260", True), "iSCSI discovery Failed")

    def test_iscsi_discovery_login_logout_incorrect_ip(self):
        """Is wrong IP returns successfully?"""
        self.assertIsNone(self.iscsi_obj._iscsi_discovery_login_logout(
            "10.11.521.234", "3260", True), "Incorrect IP address")

    def test_iscsi_discovery_login_logout_incorrect_port(self):
        """Is wrong port returns successfully?"""
        self.assertIsNone(self.iscsi_obj._iscsi_discovery_login_logout(
            "10.11.57.2", "6340", False), "Incorrect port")

    def test_get_multipath_device_success_ctrl(self):
        """Is get multipath device function returns successfully?"""
        self.assertEqual(self.iscsi_obj._get_multipath_device(
            "/dev/sdb"), '/dev/mapper/CTRL_LUN', "Get multipath device Failed")

    def test_get_multipath_device_incorrect_device(self):
        """Is wrong scsi device returns successfully?"""
        self.assertIsNone(self.iscsi_obj._get_multipath_device(
            "10.11.521.234"), "Invalid scsi device")

    def test_get_multipath_device_incorrect_device_path(self):
        """Is wrong scsi device path returns successfully?"""
        self.assertIsNone(self.iscsi_obj._get_multipath_device(
            "/opt/sdb"), "Incorrect scsi device path")

    def test_get_initiator_name_success(self):
        """Is get initiator name function returns successfully?"""
        initiator, status = self.iscsi_obj._run_command(
            "cat /etc/iscsi/initiatorname.iscsi")
        return_value = self.iscsi_obj.get_initiator_name()
        self.assertIn(return_value, initiator, "Get Initiator failed")

    def test_find_paths_success_ctrl(self):
        """Is find paths function returns successfully?"""
        ret_value = self.iscsi_obj.find_paths("d5570000")
        print"ret_value1: ", ret_value
        exptected = '/dev/mapper/CTRL_LUN'
        self.assertIn(exptected, ret_value, "Find Path Failed")

    def test_find_paths_incorrect_device(self):
        """Is wrong device returns successfully?"""
        ret_value = self.iscsi_obj.find_paths("10.11.521.234")
        print"ret_value2: ", ret_value
        self.assertEqual(ret_value, [], "Incorrect page 0x80 device id")


if __name__ == '__main__':
    unittest.main()
