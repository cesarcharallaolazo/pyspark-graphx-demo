import requests
from utils.constant import *
from utils.settings import SLACK_URL


def build_success_slack_msg(process_name=None):
    """build_success_slack_msg."""
    msg = {
        "process_number": SLACK_MESSAGES[process_name]['process_number'],
        "process_title": SLACK_MESSAGES[process_name]['process_title'],
        "process_description": f"{SLACK_MESSAGES[process_name]['process_description']} "
                               f"{SLACK_MESSAGES[process_name]['status_ok']}",
        "process_status_emoji": ":white_check_mark:",
        "error_description_title": "",
        "error_description": ""
    }
    return msg


def build_failed_slack_msg(process_name=None):
    """build_failed_slack_msg."""
    msg = {
        "process_number": SLACK_MESSAGES[process_name]['process_number'],
        "process_title": SLACK_MESSAGES[process_name]['process_title'],
        "process_description": f"{SLACK_MESSAGES[process_name]['process_description']} "
                               f"{SLACK_MESSAGES[process_name]['status_failed']}",
        "process_status_emoji": ":x:",
        "error_description_title": SLACK_MESSAGES[process_name]['error_description_title'],
        "error_description": SLACK_MESSAGES[process_name]['error_description']
    }
    return msg


def success_step_msg(process_name=None, url=SLACK_URL):
    """success_step_msg."""
    json_msg = build_success_slack_msg(process_name)
    response = requests.post(url, json=json_msg)
    if response.status_code != 200:
        print("Slack message hasn't sent !")
    else:
        print("Slack message has sent !")


def failed_step_msg(process_name=None, url=SLACK_URL):
    """failed_step_msg."""
    json_msg = build_failed_slack_msg(process_name)
    response = requests.post(url, json=json_msg)
    if response.status_code != 200:
        print("Slack message hasn't sent !")
    else:
        print("Slack message has sent !")

