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

    print(f"Server responded with status code: {response.status_code}")

    if response.status_code == 200:
        print("Success: The site is reachable from this environment.")

        try:
            print("Server response (JSON):", response.json())
        except json.JSONDecodeError as je:
            print(f"Error: Server returned status 200, but payload is invalid JSON. Details: {je}")
            print("Raw response text:", response.text)

    elif response.status_code == 429:
        print("Error HTTP 429: Rate limit exceeded. The server is rejecting requests due to high volume.")
    elif response.status_code in (500, 502, 503, 504):
        print(f"Error HTTP {response.status_code}: Temporary server-side failure.")
    else:
        print(f"Error HTTP {response.status_code}: Client or server error encountered.")
        print("Raw response text:", response.text)

except requests.exceptions.SSLError as e:
    print("Error SSL: Certificate verification failed.")
    print("Possible causes: Self-signed certificate, expired certificate, or traffic interception.")
    print("Pass 'verify=False' inside your requests.get() method to bypass this check.")

except requests.exceptions.ConnectTimeout:
    print("Error Timeout: Failed to establish a connection to the server within the limit.")
    print("Possible causes: Target host is down, wrong IP/port, or blocked by a firewall.")

except requests.exceptions.ReadTimeout:
    print("Error Timeout: Connection is established, but the server took too long to send data.")
    print("Possible causes: Server is overloaded or processing a heavy query.")
    
except requests.exceptions.ConnectionError as e:

    error_message = str(e)
    if "Failed to establish a new connection" in error_message or "Connection refused" in error_message:
        print("Error Network: Physically unable to connect to the host.")
        print("Ensure your corporate VPN is connected, the proxy settings are correct, and your IP is whitelisted.")
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

