# PYTHON VERSION

import json
import requests


try:
    # GET REQUEST
    url = "https://catfact.ninja/fact"
    id = "id"
    secret = "secret"
    params = {
        "param_name": "param_value"
    }
    response = requests.get(url, auth=(id, secret), timeout=10, params=params)

    # # POST request with a 10-second timeout to prevent hanging
    # url = "https://catfact.ninja/fact"
    # print(f"Checking connection to {url}...")
    # # Minimal payload to satisfy the API structure with minimal server load
    # payload = {"auth": {"login": "", "password": ""}, "filters": [], "quantity": {"limit": 1}}
    # headers = {"Content-Type": "application/json"}
    # response = requests.post(url, headers=headers, json=payload, timeout=10)

    print(f"Response status code: {response.status_code}")

    if response.status_code == 200:
        print("Success! The site is reachable from this environment.")

        try:
            print("Server response (JSON):", response.json())
        except json.JSONDecodeError:
            print("Response received, but it is not valid JSON (could be an error page).")
        else:
            print(f"🔴 Server Error: Responded with status code {response.status_code}")
            print("Raw response text:", response.text)

except requests.exceptions.Timeout:
    # connection is blocked from the external firewall
    print("Error: Connection timed out. The server did not respond within 10 seconds.")
except requests.exceptions.ConnectionError as e:
    # Convert the entire exception chain to a string to inspect the root cause
    error_message = str(e)

    # 1. Catch SSL Certificate Verification failures (like Self-Signed certificates)
    if "SSLError" in error_message or "certificate verify failed" in error_message:
        print("Error: failed due to a strict certificate verification check!")
        print("Pass 'verify=False' inside your requests.get() method to bypass this check.")

    # 2. Catch physical network, DNS, or VPN connectivity issues
    elif "Failed to establish a new connection" in error_message or "Connection refused" in error_message:
        print("Failed to physically establish a connection to the host.")
        print( "Ensure your corporate VPN is connected, the proxy settings are correct, and your IP is whitelisted.")

    # 3. Fallback for any other type of ConnectionError
    else:
        print(f"General Connection Error occurred: {e}")

except requests.exceptions.RequestException as e:
    print(f"An unexpected error occurred: {e}")


# CURL VERSION (Databricks)

# %sh
# curl -k -I "https://catfact.ninja/fact"


# PING VERSION (Databricks), ping might be blocked on the server leve lso we should use curl or python

# %sh
# ping -c 4 "catfact.ninja/fact"

