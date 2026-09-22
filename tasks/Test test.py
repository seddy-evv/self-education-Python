import requests
import urllib3

# Suppress only the InsecureRequestWarning if you choose to use verify=False later.
# For now, we leave it active so we can catch and diagnose the error.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://api-qa.np.six-group.net/vns/v1/valor-range/current"
credentials = ("your_id", "your_secret")

try:
    # Standard request without verify=False to simulate the actual test environment
    response = requests.get(url, auth=credentials, timeout=10)
    
    if response.status_code == 200:
        print("🟢 Success! The server responded with 200 OK.")
        try:
            print("Server response (JSON):", response.json())
        except (ValueError, KeyError, TypeError):
            print("Response received, but it is not a valid JSON structure.")
            print("Raw response text:", response.text)
    else:
        print(f"🔴 Server Error: Responded with status code {response.status_code}")
        print("Raw response text:", response.text)

except requests.exceptions.Timeout:
    print("⏳ Error: Connection timed out. The server did not respond within 10 seconds.")
    print("👉 POSSIBLE CAUSE: The connection might be silently blocked by an external firewall.")

except requests.exceptions.ConnectionError as e:
    # Convert the entire exception chain to a string to inspect the root cause
    error_message = str(e)
    
    # 1. Catch SSL Certificate Verification failures (like Self-Signed certificates)
    if "SSLError" in error_message or "certificate verify failed" in error_message:
        print("🔒 [SSL ERROR]: Test failed due to a strict certificate verification check!")
        print("👉 DIAGNOSIS: The server is using a self-signed or untrusted certificate (common in QA environments).")
        print("👉 FIX: Pass 'verify=False' inside your requests.get() method to bypass this check.")
    
    # 2. Catch physical network, DNS, or VPN connectivity issues
    elif "Failed to establish a new connection" in error_message or "Connection refused" in error_message:
        print("🌐 [NETWORK ERROR]: Failed to physically establish a connection to the host.")
        print("👉 DIAGNOSIS: The target server is unreachable on this port.")
        print("👉 FIX: Ensure your corporate VPN is connected, the proxy settings are correct, and your IP is whitelisted.")
    
    # 3. Fallback for any other type of ConnectionError
    else:
        print(f"💥 General Connection Error occurred: {e}")

except requests.exceptions.RequestException as e:
    print(f"🛑 Unexpected RequestException occurred: {e}")
