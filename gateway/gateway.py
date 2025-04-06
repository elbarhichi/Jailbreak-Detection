import requests
import json
import time
import pandas as pd
import os
import uuid

url = os.getenv("GATEWAY_URL", "http://localhost:5000/send")

df = pd.read_csv('test.csv')
messages = df['prompt'].tolist()

for msg in messages:
    unique_id = str(uuid.uuid4())
    payload = {"id": unique_id, "message": msg}
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    
    if response.status_code == 200:
        print(f"Message '{msg}' avec id {unique_id} envoyé avec succès.")
    else:
        print(f"Erreur lors de l'envoi du message '{msg}' avec id {unique_id}: {response.text}")
    
    time.sleep(10)
