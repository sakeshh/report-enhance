import requests
import json
import os

env_path = r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend\.env"
token = None
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("BACKEND_AUTH_TOKEN="):
                token = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

url = "http://127.0.0.1:8000/etl/infer-semantics"
payload = {
    "sources": ["dbo.Customers_Raw", "dbo.Orders_Raw"]
}

headers = {}
if token:
    headers["X-Backend-Token"] = token
    print("Found token in .env, using it.")
else:
    print("WARNING: BACKEND_AUTH_TOKEN not found in .env")

try:
    print("Calling /etl/infer-semantics endpoint with token...")
    response = requests.post(url, json=payload, headers=headers)
    print("Status Code:", response.status_code)
    try:
        print("Response Body JSON:", json.dumps(response.json(), indent=2))
    except Exception:
        print("Response Body Text:", response.text)
except Exception as e:
    print("Error calling endpoint:", e)
