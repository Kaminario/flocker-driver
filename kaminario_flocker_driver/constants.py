""" Defines constant """

DRIVER_NAME = u"kaminario_flocker_driver"  # K2 driver name
# This is define to control looping for multi-path discovery
ITERATION_LIMIT = 4
DELAY = 5  # it is in secs
RESCAN_DELAY = 3  # it is in secs
TRUE_EXP = [1, '1', 'true', True]  # is used to define true expression
UNLIMITED_QUOTA = 0  #Unlimited quota for volume group

# Character limit for K2 VG name is: 42 chars
# dataset_id includes 32 chars and 4 hyphens(total 36 chars)
VG_PREFIX = "K2FVG"  # Character limit for VG prefix is: 5 chars
VOL_PREFIX = "K2F"  # Volume name prefix
LEN_OF_DATASET_ID = 36  # Length of dataset id
RETRIES = 5  # Retries count to add delay in krest calls to "Too many request"
