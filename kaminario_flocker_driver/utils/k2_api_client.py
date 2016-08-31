""" This is k2_api_client docstring """
import logging
import functools
import platform
import bitmath
import krest
import threading
import time
import ast
from requests.exceptions import HTTPError
from kaminario_flocker_driver.utils.iscsi_utils import IscsiUtils
from kaminario_flocker_driver.constants import TRUE_EXP

LOG = logging.getLogger(__name__)


class FunctionalUtility(IscsiUtils):
    """Functional Utilities for API objects.

    Defines general utility purpose.
    """

    @staticmethod
    def bytes_to_kib(size):
        """Convert size in bytes to KiB.

        :param size: The number of bytes.
        :returns: The size in Kilobytes.
        """
        return bitmath.Byte(size).to_KiB().value

    @staticmethod
    def kib_to_bytes(size):
        """Convert  KiB to size in bytes.

        :param size: The number of Kilobytes.
        :returns: The size in Kilobytes.
        """
        return bitmath.KiB(size).to_Byte().value

    @staticmethod
    def is_true(flag):
        """ Validate True expression.

        :param flag: status of True or False.
        :returns: True if flag is set [1, 'true', 'True', True] or False.
        """
        if isinstance(flag, str):
            flag = flag.lower()
        if flag in TRUE_EXP:
            return True
        else:
            return False

    @property
    def host_type(self):
        """Returns hostname

        :returns: hostname.
        """
        return unicode(platform.uname()[0])

    @staticmethod
    def rgetattr(obj, attr, default=object()):
        """Search an attribute inside the Krest object and
           return its value.

        :param obj: Krest object
        :param attr: attribute name to be searched from Krest object
        :param default: python object
        :return: Value of attribute searched.
        """
        if default is object():
            _getattr = getattr
        else:
            def _getattr(obj, name):
                """ Get an attribute from Krest object"""
                return getattr(obj, name, default)
        return functools.reduce(_getattr, [obj]+attr.split('.'))

    @staticmethod
    def get_attr_list(query):
        """Make list if attributes
        :param query: attribute name to be searched from Krest object
        :return: list attribute name
        """
        attr_list = []
        if isinstance(query, dict):
            for k, v in query.items():
                ref_keys = k.split('__')
                attr_name = ".".join(ref_keys)
                attr_list.append({attr_name: v})
        return attr_list

    def advance_search(self, resource_data, **query):
        """Advance Search
        :param resource_data: Krest objects
        :param query: attribute name to be searched from Krest object
        :return: match record.
        """
        result_set = []
        return_set = []
        attr_list = self.get_attr_list(query)
        for data in resource_data:
            for attr in attr_list:
                if self.rgetattr(data, attr.keys()[0]) == attr.values()[0]:
                    result_set.append(True)
                else:
                    result_set.append(False)
                    break
            if all(result_set):
                return_set.append(data)
            else:
                del result_set[:]
        return return_set


class KrestExtendedEndPoint(krest.EndPoint):
    """This is extended class of Krest EndPoint

    Added logic to avoid "Too Many requiest"
    As provided by kaminario
    """
    instances = []  # list of class instances

    def __init__(self, *args, **kwargs):
        self.krestlock = threading.Lock()
        KrestExtendedEndPoint.instances.append(self)
        if "retries" in kwargs:
            self.retries = int(kwargs["retries"])
            del kwargs["retries"]

        super(KrestExtendedEndPoint, self).__init__(*args, **kwargs)

    @staticmethod
    def _should_retry(status_code, message):
        LOG.info("Instaces count %d", len(KrestExtendedEndPoint.instances))
        LOG.info("should retry ERROR %s status code %d", message, status_code)
        if status_code == 400 and \
                (message == "MC_ERR_BUSY" or
                    message == "MC_ERR_BUSY_SPECIFIC" or
                    message == "MC_ERR_INPROGRESS" or
                    message == "MC_ERR_START_TIMEOUT"):
            return True
        else:
            return False

    def _request(self, method, *args, **kwargs):
        i = 0
        while i < self.retries:
            if i > 0:
                time.sleep(1)
            try:
                LOG.info("running through the _request wrapper...")
                self.krestlock.acquire()
                return super(KrestExtendedEndPoint, self)._request(
                    method, *args, **kwargs)
            except HTTPError as ex:
                if self._should_retry(ex.response.status_code, ast.literal_eval(
                        ex.response.text)['error_msg']):
                    i += 1
                    continue
                else:
                    raise Exception('%s' % ex.response.text)
            except Exception as ex:
                raise Exception('%s' % ex.message)
            finally:
                self.krestlock.release()


class K2StorageCenterApi(FunctionalUtility):
    """
    K2StorageCenterApi class for API access.  Handles opening the
    connection to the K2 Storage Center.
    """

    def __init__(self, host, username, password, is_ssl=False, retries=None):
        """This will initiate a connection to K2 storage device.

        :param host: IP address of the K2 Storage device.
        :param port: Port the Data Collector is listening on.
        :param username: Username to login with.
        :param password: Password.
        :param ssl: Boolean indicating whether certificate verification
                       should be turned on or not.
        :param retries: It is used add a delay in Krest calls
        """
        self.host = host
        self.username = username
        self.password = password
        self.is_ssl = self.is_true(is_ssl)
        self.retries = retries

    def connect_to_api(self):
        """It will connect to K2 API layer.

        :return: krest end point object.
        :raises: StorageDriverAPIException
        """
        try:
            ep = KrestExtendedEndPoint(self.host, self.username,
                                       self.password, ssl_validate=self.is_ssl,
                                       retries=self.retries)
        except Exception as e:
            raise StorageDriverAPIException('K2 API connection failure: {}'.
                                            format(e))
        return ep


class StorageDriverAPIException(Exception):
    """K2(krest) backend API exception."""


class InvalidDataException(Exception):
    """Invalid data exception.

    It is raise, when data or information is not sufficient to perform
    operation or invalid data is supplied
    """


class ImproperConfigurationError(Exception):
    """agent.yml file Dataset configuration exception"""
