import configparser
import itertools
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Union

import oci
import paramiko
from dotenv import load_dotenv
import requests

# Load environment variables from .env file
load_dotenv('oci.env')

ARM_SHAPE = "VM.Standard.A1.Flex"
E2_MICRO_SHAPE = "VM.Standard.E2.1.Micro"

# Access loaded environment variables and strip white spaces
OCI_CONFIG = os.getenv("OCI_CONFIG", "").strip()
OCT_FREE_AD = os.getenv("OCT_FREE_AD", "").strip()
DISPLAY_NAME = os.getenv("DISPLAY_NAME", "").strip()
WAIT_TIME = int(os.getenv("REQUEST_WAIT_TIME_SECS", "0").strip())
OCI_IMAGE_ID = os.getenv("OCI_IMAGE_ID", None).strip() if os.getenv("OCI_IMAGE_ID") else None
OCI_COMPUTE_SHAPE = os.getenv("OCI_COMPUTE_SHAPE", ARM_SHAPE).strip()
SECOND_MICRO_INSTANCE = os.getenv("SECOND_MICRO_INSTANCE", 'False').strip().lower() == 'true'
OPERATING_SYSTEM = os.getenv("OPERATING_SYSTEM", "").strip()
OS_VERSION = os.getenv("OS_VERSION", "").strip()
ASSIGN_PUBLIC_IP = os.getenv("ASSIGN_PUBLIC_IP", "false").strip()
BOOT_VOLUME_SIZE = os.getenv("BOOT_VOLUME_SIZE", "50").strip()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()
VCN_NAME = os.getenv("VCN_NAME", "").strip()
SUBNET_NAME = os.getenv("SUBNET_NAME", "").strip()
BOOT_VOLUME_NAME = os.getenv("BOOT_VOLUME_NAME", "").strip()

# Read the configuration from oci_config file
config = configparser.ConfigParser()
try:
    config.read(OCI_CONFIG)
    OCI_USER_ID = config.get('DEFAULT', 'user')
    if OCI_COMPUTE_SHAPE not in (ARM_SHAPE, E2_MICRO_SHAPE):
        raise ValueError(f"{OCI_COMPUTE_SHAPE} is not an acceptable shape")
    env_has_spaces = any(isinstance(confg_var, str) and " " in confg_var
                        for confg_var in [OCI_CONFIG, OCT_FREE_AD,WAIT_TIME,
                                OCI_IMAGE_ID, 
                                OCI_COMPUTE_SHAPE, SECOND_MICRO_INSTANCE, 
                                OS_VERSION, DISCORD_WEBHOOK, VCN_NAME, SUBNET_NAME, BOOT_VOLUME_NAME]
                        )
    config_has_spaces = any(' ' in value for section in config.sections() 
                            for _, value in config.items(section))
    if env_has_spaces:
        raise ValueError("oci.env has spaces in values which is not acceptable")
    if config_has_spaces:
        raise ValueError("oci_config has spaces in values which is not acceptable")        

except configparser.Error as e:
    with open("ERROR_IN_CONFIG.log", "w", encoding='utf-8') as file:
        file.write(str(e))

    print(f"Error reading the configuration file: {e}")

# Set up logging
logging.basicConfig(
    filename="setup_and_info.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging_step5 = logging.getLogger("launch_instance")
logging_step5.setLevel(logging.INFO)
fh = logging.FileHandler("launch_instance.log")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging_step5.addHandler(fh)

# Set up OCI Config and Clients
oci_config_path = OCI_CONFIG if OCI_CONFIG else "~/.oci/config"
config = oci.config.from_file(oci_config_path)
iam_client = oci.identity.IdentityClient(config)
network_client = oci.core.VirtualNetworkClient(config)
compute_client = oci.core.ComputeClient(config)
blockstorage_client = oci.core.BlockstorageClient(config)

IMAGE_LIST_KEYS = [
    "lifecycle_state",
    "display_name",
    "id",
    "operating_system",
    "operating_system_version",
    "size_in_mbs",
    "time_created",
]


def write_into_file(file_path, data):
    """Write data into a file.

    Args:
        file_path (str): The path of the file.
        data (str): The data to be written into the file.
    """
    with open(file_path, mode="a", encoding="utf-8") as file_writer:
        file_writer.write(data)


def list_all_instances(compartment_id):
    """Retrieve a list of all instances in the specified compartment.

    Args:
        compartment_id (str): The compartment ID.

    Returns:
        list: The list of instances returned from the OCI service.
    """
    list_instances_response = compute_client.list_instances(compartment_id=compartment_id)
    return list_instances_response.data


def create_instance_details_file_and_notify(instance, shape=ARM_SHAPE):
    """Create a file with details of instances and notify the user.

    Args:
        instance (dict): The instance dictionary returned from the OCI service.
        shape (str): shape of the instance to be created, acceptable values are
         "VM.Standard.A1.Flex", "VM.Standard.E2.1.Micro"
    """
    details = [f"Instance ID: {instance.id}",
               f"Display Name: {instance.display_name}",
               f"Availability Domain: {instance.availability_domain}",
               f"Shape: {instance.shape}",
               f"State: {instance.lifecycle_state}",
               "\n"]
    micro_body = 'TWo Micro Instances are already existing and running'
    arm_body = '\n'.join(details)
    body = arm_body if shape == ARM_SHAPE else micro_body
    write_into_file('INSTANCE_CREATED', body)


def notify_on_failure(failure_msg):
    """Notifies users when the Instance Creation Failed due to an error that's
    not handled.

    Args:
        failure_msg (msg): The error message.
    """

    mail_body = (
        "The script encountered an unhandled error and exited unexpectedly.\n\n"
        "Please re-run the script by executing './setup_init.sh rerun'.\n\n"
        "And raise a issue on GitHub if its not already existing:\n"
        "https://github.com/mohankumarpaluru/oracle-freetier-instance-creation/issues\n\n"
        " And include the following error message to help us investigate and resolve the problem:\n\n"
        f"{failure_msg}"
    )
    write_into_file('UNHANDLED_ERROR.log', mail_body)


def check_instance_state_and_write(compartment_id, shape, states=('RUNNING', 'PROVISIONING'),
                                   tries=3):
    """Check the state of instances in the specified compartment and take action when a matching instance is found.

    Args:
        compartment_id (str): The compartment ID to check for instances.
        shape (str): The shape of the instance.
        states (tuple, optional): The lifecycle states to consider. Defaults to ('RUNNING', 'PROVISIONING').
        tries(int, optional): No of reties until an instance is found. Defaults to 3.

    Returns:
        bool: True if a matching instance is found, False otherwise.
    """
    for _ in range(tries):
        instance_list = list_all_instances(compartment_id=compartment_id)
        if shape == ARM_SHAPE:
            running_arm_instance = next((instance for instance in instance_list if
                                         instance.shape == shape and instance.lifecycle_state in states), None)
            if running_arm_instance:
                create_instance_details_file_and_notify(running_arm_instance, shape)
                return True
        else:
            micro_instance_list = [instance for instance in instance_list if
                                   instance.shape == shape and instance.lifecycle_state in states]
            if len(micro_instance_list) > 1 and SECOND_MICRO_INSTANCE:
                create_instance_details_file_and_notify(micro_instance_list[-1], shape)
                return True
            if len(micro_instance_list) == 1 and not SECOND_MICRO_INSTANCE:
                create_instance_details_file_and_notify(micro_instance_list[-1], shape)
                return True       
        if tries - 1 > 0:
            time.sleep(60)

    return False


def handle_errors(command, data, log):
    """Handles errors and logs messages.

    Args:
        command (arg): The OCI command being executed.
        data (dict): The data or error information returned from the OCI service.
        log (logging.Logger): The logger instance for logging messages.

    Returns:
        bool: True if the error is temporary and the operation should be retried after a delay.
        Raises Exception for unexpected errors.
    """

    # Check for temporary errors that can be retried
    if "code" in data:
        if (data["code"] in ("TooManyRequests", "Out of host capacity.", 'InternalError')) \
                or (data["message"] in ("Out of host capacity.", "Bad Gateway")):
            log.info("Command: %s--\nOutput: %s", command, data)
            time.sleep(WAIT_TIME)
            return True

    if "status" in data and data["status"] == 502:
        log.info("Command: %s~~\nOutput: %s", command, data)
        time.sleep(WAIT_TIME)
        return True
    failure_msg = '\n'.join([f'{key}: {value}' for key, value in data.items()])
    notify_on_failure(failure_msg)
    # Raise an exception for unexpected errors
    raise Exception("Error: %s" % data)


def execute_oci_command(client, method, *args, **kwargs):
    """Executes an OCI command using the specified OCI client.

    Args:
        client: The OCI client instance.
        method (str): The method to call on the OCI client.
        args: Additional positional arguments to pass to the OCI client method.
        kwargs: Additional keyword arguments to pass to the OCI client method.

    Returns:
        dict: The data returned from the OCI service.

    Raises:
        Exception: Raises an exception if an unexpected error occurs.
    """
    while True:
        try:
            response = getattr(client, method)(*args, **kwargs)
            data = response.data if hasattr(response, "data") else response
            return data
        except oci.exceptions.ServiceError as srv_err:
            data = {"status": srv_err.status,
                    "code": srv_err.code,
                    "message": srv_err.message}
            handle_errors(args, data, logging_step5)


def send_discord_message(message):
    """Send a message to Discord using the webhook URL if available."""
    if DISCORD_WEBHOOK:
        payload = {"content": message}
        try:
            response = requests.post(DISCORD_WEBHOOK, json=payload)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error("Failed to send Discord message: %s", e)


def search_for_resource(client, method, compartment_id, resource_name=None):
    """Search for a resource by name or return the first resource if no name is provided.

    Args:
        client: The OCI client instance.
        method (str): The method to call on the OCI client.
        compartment_id (str): The compartment ID to search in.
        resource_name (str, optional): The name of the resource to search for. Defaults to None.

    Returns:
        dict: The resource data returned from the OCI service.
    """
    resources = execute_oci_command(client, method, compartment_id=compartment_id)
    if resource_name:
        resource = next((res for res in resources if res.display_name == resource_name), None)
    else:
        resource = resources[0] if resources else None
    return resource


def launch_instance():
    """Launches an OCI Compute instance using the specified parameters.

    Raises:
        Exception: Raises an exception if an unexpected error occurs.
    """
    # Step 1 - Get TENANCY
    user_info = execute_oci_command(iam_client, "get_user", OCI_USER_ID)
    oci_tenancy = user_info.compartment_id
    logging.info("OCI_TENANCY: %s", oci_tenancy)

    # Step 2 - Get AD Name
    availability_domains = execute_oci_command(iam_client,
                                               "list_availability_domains",
                                               compartment_id=oci_tenancy)
    oci_ad_name = [item.name for item in availability_domains if
                   any(item.name.endswith(oct_ad) for oct_ad in OCT_FREE_AD.split(","))]
    oci_ad_names = itertools.cycle(oci_ad_name)
    logging.info("OCI_AD_NAME: %s", oci_ad_name)

    # Step 3 - Get VCN ID
    vcn = search_for_resource(network_client, "list_vcns", oci_tenancy, VCN_NAME)
    oci_vcn_id = vcn.id if vcn else None
    logging.info("OCI_VCN_ID: %s", oci_vcn_id)

    # Step 4 - Get Subnet ID
    subnet = search_for_resource(network_client, "list_subnets", oci_tenancy, SUBNET_NAME)
    oci_subnet_id = subnet.id if subnet else None
    logging.info("OCI_SUBNET_ID: %s", oci_subnet_id)

    # Step 5 - Get Boot Volume ID
    boot_volume = search_for_resource(blockstorage_client, "list_boot_volumes", oci_tenancy, BOOT_VOLUME_NAME)
    oci_boot_volume_id = boot_volume.id if boot_volume else None
    logging.info("OCI_BOOT_VOLUME_ID: %s", oci_boot_volume_id)

    # Step 6 - Get Image ID of Compute Shape
    if not OCI_IMAGE_ID:
        images = execute_oci_command(
            compute_client,
            "list_images",
            compartment_id=oci_tenancy,
            shape=OCI_COMPUTE_SHAPE,
        )
        shortened_images = [{key: json.loads(str(image))[key] for key in IMAGE_LIST_KEYS
                             } for image in images]
        write_into_file('images_list.json', json.dumps(shortened_images, indent=2))
        oci_image_id = next(image.id for image in images if
                            image.operating_system == OPERATING_SYSTEM and
                            image.operating_system_version == OS_VERSION)
        logging.info("OCI_IMAGE_ID: %s", oci_image_id)
    else:
        oci_image_id = OCI_IMAGE_ID

    assign_public_ip = ASSIGN_PUBLIC_IP.lower() in [ "true", "1", "y", "yes" ]

    boot_volume_size = max(50, int(BOOT_VOLUME_SIZE))

    # Step 7 - Launch Instance if it's not already exist and running
    instance_exist_flag = check_instance_state_and_write(oci_tenancy, OCI_COMPUTE_SHAPE, tries=1)

    if OCI_COMPUTE_SHAPE == "VM.Standard.A1.Flex":
        shape_config = oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=4, memory_in_gbs=24)
    else:
        shape_config = oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=1, memory_in_gbs=1)

    while not instance_exist_flag:
        try:
            if oci_boot_volume_id:
                source_details = oci.core.models.InstanceSourceViaBootVolumeDetails(
                    source_type="bootVolume",
                    boot_volume_id=oci_boot_volume_id
                )
            else:
                source_details = oci.core.models.InstanceSourceViaImageDetails(
                    source_type="image",
                    image_id=oci_image_id,
                    boot_volume_size_in_gbs=boot_volume_size,
                )

            launch_instance_response = compute_client.launch_instance(
                launch_instance_details=oci.core.models.LaunchInstanceDetails(
                    availability_domain=next(oci_ad_names),
                    compartment_id=oci_tenancy,
                    create_vnic_details=oci.core.models.CreateVnicDetails(
                        assign_public_ip=assign_public_ip,
                        assign_private_dns_record=True,
                        display_name=DISPLAY_NAME,
                        subnet_id=oci_subnet_id,
                    ),
                    display_name=DISPLAY_NAME,
                    shape=OCI_COMPUTE_SHAPE,
                    availability_config=oci.core.models.LaunchInstanceAvailabilityConfigDetails(
                        recovery_action="RESTORE_INSTANCE"
                    ),
                    instance_options=oci.core.models.InstanceOptions(
                        are_legacy_imds_endpoints_disabled=False
                    ),
                    shape_config=shape_config,
                    source_details=source_details,
                    metadata={},
                )
            )
            if launch_instance_response.status == 200:
                logging_step5.info(
                    "Command: launch_instance\nOutput: %s", launch_instance_response
                )
                instance_exist_flag = check_instance_state_and_write(oci_tenancy, OCI_COMPUTE_SHAPE)

        except oci.exceptions.ServiceError as srv_err:
            if srv_err.code == "LimitExceeded":                
                logging_step5.info("Encoundered LimitExceeded Error checking if instance is created" \
                                   "code :%s, message: %s, status: %s", srv_err.code, srv_err.message, srv_err.status)                
                instance_exist_flag = check_instance_state_and_write(oci_tenancy, OCI_COMPUTE_SHAPE)
                if instance_exist_flag:
                    logging_step5.info("%s , exiting the program", srv_err.code)
                    sys.exit()
                logging_step5.info("Didn't find an instance , proceeding with retries")     
            data = {
                "status": srv_err.status,
                "code": srv_err.code,
                "message": srv_err.message,
            }
            handle_errors("launch_instance", data, logging_step5)

        time.sleep(60)  # Wait for 60 seconds before the next attempt


if __name__ == "__main__":
    send_discord_message("🚀 OCI Instance Creation Script: Starting up! Let's create some cloud magic!")
    try:
        launch_instance()
        send_discord_message("🎉 Success! OCI Instance has been created. Time to celebrate!")
    except Exception as e:
        error_message = f"😱 Oops! Something went wrong with the OCI Instance Creation Script:\n{str(e)}"
        send_discord_message(error_message)
        raise
