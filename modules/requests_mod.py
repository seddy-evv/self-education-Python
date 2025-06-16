# The requests library is used for making HTTP requests, enabling you to interact with APIs and web resources.
import requests

# 1. requests.get(url, params=None, **kwargs)
# Sends a GET request to the specified URL. Used to retrieve data from a server.
# Important Parameters:
# url		    Required. The url of the request
# params		Optional. A dictionary, list of tuples or bytes to send as a query string. Default None
# auth		    Optional. A tuple to enable a certain HTTP authentication. Default None
# cert		    Optional. A String or Tuple specifying a cert file or key. Default None
# cookies		Optional. A dictionary of cookies to send to the specified url. Default None
# headers		Optional. A dictionary of HTTP headers to send to the specified url. Default None
# proxies		Optional. A dictionary of the protocol to the proxy url. Default None
# timeout		Optional. A number, or a tuple, indicating how many seconds to wait for the client to make a
#                         connection and/or send a response. Default None which means the request will continue until
#                         the connection is closed

response = requests.get("https://api.example.com/data", params={"key": "value"})
print(response.status_code)  # HTTP status code
print(response.json())       # Parse response JSON

# 2. requests.post(url, data=None, json=None, **kwargs)
# Sends a POST request to the specified URL. Typically used to send data to a server.
#
# Use 'data' for form-encoded data.
# Use 'json' for JSON-encoded data.
# Important Parameters:
# url		    Required. The url of the request
# data		    Optional. A dictionary, list of tuples, bytes or a file object to send to the specified url
# json		    Optional. A JSON object to send to the specified url
# files		    Optional. A dictionary of files to send to the specified url
# auth		    Optional. A tuple to enable a certain HTTP authentication. Default None
# cert		    Optional. A String or Tuple specifying a cert file or key. Default None
# cookies		Optional. A dictionary of cookies to send to the specified url. Default None
# headers		Optional. A dictionary of HTTP headers to send to the specified url. Default None
# proxies		Optional. A dictionary of the protocol to the proxy url. Default None
# timeout		Optional. A number, or a tuple, indicating how many seconds to wait for the client to make a
#                         connection and/or send a response. Default None which means the request will continue until
#                         the connection is closed

# When making HTTP requests with the Python requests library, the Content-Type header specifies the format of the
# data being sent in the request body. It's crucial for the server to correctly interpret the request.
# The requests library usually sets the Content-Type header automatically based on the data being sent. However,
# it can be explicitly set or overridden using the headers parameter in the request methods:
# Setting Content-Type to application/json
headers = {'Content-Type': 'application/json'}
data = {'key': 'value'}
response = requests.post('url', json=data, headers=headers)

# Setting Content-Type to application/xml
headers = {'Content-Type': 'application/xml'}
xml_data = '<xml><key>value</key></xml>'
response = requests.post('url', data=xml_data, headers=headers)

# Sending form data
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
form_data = {'param1': 'value1', 'param2': 'value2'}
response = requests.post('url', data=form_data, headers=headers)

# Sending files
files = {'file': open('file.txt', 'rb')}
response = requests.post('url', files=files) # Content-Type is automatically set to multipart/form-data

# Example for post request:
payload = {"username": "user", "password": "123456"}
response = requests.post("https://api.example.com/login", json=payload)
print(response.status_code)
print(response.json())

# 3. requests.put(url, data=None, **kwargs)
# Sends a PUT request to update or replace a resource on the server.
updated_data = {"name": "John", "age": 30}
response = requests.put("https://api.example.com/users/1", json=updated_data)
print(response.status_code)
print(response.json())

# 4. requests.delete(url, **kwargs)
# Sends a DELETE request to delete a resource from the server (file, record etc).
# Important Parameters:
# url		    Required. The url of the request
# auth		    Optional. A tuple to enable a certain HTTP authentication. Default None
# cert		    Optional. A String or Tuple specifying a cert file or key. Default None
# cookies		Optional. A dictionary of cookies to send to the specified url. Default None
# headers		Optional. A dictionary of HTTP headers to send to the specified url. Default None
# proxies		Optional. A dictionary of the protocol to the proxy url. Default None
# stream		Optional. A Boolean indication if the response should be immediately downloaded (False) or streamed (True).
# timeout		Optional. A number, or a tuple, indicating how many seconds to wait for the client to make a
#                         connection and/or send a response. Default None which means the request will continue until
#                         the connection is closed

response = requests.delete("https://api.example.com/users/1")
print(response.status_code)

# 5. requests.head(url, **kwargs)
# Sends a HEAD request to retrieve headers for a resource without the response body.
# Useful for checking metadata (like content type, size) or the status_code.
response = requests.head("https://api.example.com/data")
print(response.headers)  # Print response headers

# 6. requests.patch(url, data=None, **kwargs)
# Sends a PATCH request to partially update a resource on the server.
partial_update = {"age": 35}
response = requests.patch("https://api.example.com/users/1", json=partial_update)
print(response.status_code)
print(response.json())

# 7. requests.options(url, **kwargs)
# Sends an OPTIONS request to describe the communication options available for a resource or server.
response = requests.options("https://api.example.com/data")
print(response.headers)  # Check what options the server supports

# Additional Notes:
# response.status_code: Gets the HTTP status code (e.g., 200 OK, 404 Not Found).
# response.json(): Parses the response as JSON if applicable.
# response.text: Returns the raw response content as a string.
# response.headers: Access the response headers.

# Summary of Methods:
# requests.get: Retrieve data.
# requests.post: Send data or create a resource.
# requests.put: Update/replace a resource.
# requests.delete: Remove a resource.
# requests.head: Fetch headers only, no body.
# requests.patch: Partially update a resource.
# requests.options: Get communication options for a resource.
