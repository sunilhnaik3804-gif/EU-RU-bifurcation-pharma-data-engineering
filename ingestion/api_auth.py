import os
import requests


def get_api_data(api_url):

    # Read API token from environment variable
    api_token = os.getenv("PHARMA_API_TOKEN")

    if not api_token:
        raise ValueError("API token is missing")

    # API Authentication
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    # Send GET Request
    response = requests.get(
        api_url,
        headers=headers,
        timeout=30
    )

    # Check API Response
    response.raise_for_status()

    # Convert JSON response into Python data
    data = response.json()

    return data
