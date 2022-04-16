import requests
import json

response = requests.get("https://jsonplaceholder.typicode.com/photos")
json_string = response.json();
file = open("bcsample.json", "wb")
file.write(response.content)
file.close()
