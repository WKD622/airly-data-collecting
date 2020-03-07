import requests

from src.consts import AIRLY_API_KEY


def get_url(installation_id):
    return f"https://airapi.airly.eu/v2/measurements/installation?includeWind=true&installationId={installation_id}"


def make_history_data_request(installation_id):
    url = get_url(installation_id)
    response = requests.get(url=url, headers={'apikey': AIRLY_API_KEY})
    if response.status_code == 200:
        return response.json()['history']
