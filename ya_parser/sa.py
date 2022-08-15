import requests

r = requests.delete(r"http://51.250.75.50:4444/se/grid/node/session/ee88d59c-1456-44b5-bcd0-ab4e9ed9c509", headers={"X-REGISTRATION-SECRET": ""})
print(r.text)