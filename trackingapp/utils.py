import base64
from datetime import timedelta

import requests
from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone

from tracking.models import Notification, UPRRToken
from tracking.models import ScacCodes


def get_api_token():
    credentials = f"{settings.UPRR_USERNAME}:{settings.UPRR_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.post(
        settings.UPRR_TOKEN_URL,
        headers=headers,
        data={"grant_type": "client_credentials"},
    )
    if response.status_code == 200:
        token_data = response.json()
        create_token = UPRRToken.objects.create(
            access_token=token_data["access_token"],
            expires_in=token_data["expires_in"],
            token_type=token_data["token_type"],
        )
        return create_token.access_token
    else:
        print(f"Token request failed with status code {response.status_code}")
        return response.text


def get_or_create_uprr_token():
    if UPRRToken.objects.all().exists():
        token = UPRRToken.objects.last()
        expired_time = token.created_at + timedelta(seconds=int(token.expires_in))
        if expired_time >= timezone.now():
            return token.access_token
        else:
            return get_api_token()
    else:
        return get_api_token()


def get_cnrr_api_access_token():
    API_USERNAME = settings.CNRR_USERNAME
    API_PASSWORD = settings.CNRR_PASSWORD
    CREDENTIALS = f"{API_USERNAME}:{API_PASSWORD}"
    ENCODED_CREDENTIALS = base64.b64encode(CREDENTIALS.encode()).decode()
    HEADERS = {
        "Authorization": f"Basic {ENCODED_CREDENTIALS}",
        "Content-Type": "application/x-www-form-urlencoded",
        "x-apikey": settings.CNRR_API_KEY,
    }
    response = requests.post(
        settings.CNRR_TOKEN_URL,
        headers=HEADERS,
        data={"grant_type": "client_credentials"},
    )
    return response.json()


def send_error_email(website_name, html):
    emails = (
        Notification.objects.filter(name="bcc_mail")
        .values_list("email_list", flat=True)
        .first()
    )
    recipient_list = emails.split(",")
    send_mail(
        subject=f"[tracking] Trace Error Notification - {website_name}",
        message=html,
        from_email="dun.system.messages@client.com",
        recipient_list=recipient_list,
        html_message=html,
    )

def check_duplicates(data_to_trace, logger):
    response = []
    containers = []
    for entry in data_to_trace:
        if not entry['SITE_ID'].startswith('L'):
            response.append(entry)
        if containers and entry['CONTAINER_NUMBER'] in containers:
            logger.warn(
                f"Container # {entry['CONTAINER_NUMBER']}, freight bill # {entry['BILL_NUMBER']}: listed multiple times"
            )
        containers.append(entry['CONTAINER_NUMBER'])
    return response


def remove_scaccode(data_to_trace):
    scac_codes = list(ScacCodes.objects.values_list("client_id", flat=True))
    for trace_item in data_to_trace:
        if equipment_id := trace_item.get("BILL_OF_LADING"):
            equipment_id = equipment_id.strip()
            if len(equipment_id) > 3:
                equipment_id = equipment_id[:4]
                if equipment_id in scac_codes:
                    trace_item['BILL_OF_LADING'] = trace_item['BILL_OF_LADING'][4:]
    return data_to_trace
