import requests
import json
import time
import pandas as pd

# URL de votre gateway Flask
url = "http://localhost:5000/send"

# Lecture du fichier CSV et extraction des messages (colonne "prompt")
df = pd.read_csv('test.csv')
messages = df['prompt'].tolist()[80:85]

# Envoi des messages avec leur index en tant qu'ID
for idx, msg in enumerate(messages):
    payload = {"id": idx, "message": msg}
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    
    if response.status_code == 200:
        print(f"Message '{msg}' avec id {idx} envoyé avec succès.")
    else:
        print(f"Erreur lors de l'envoi du message '{msg}' avec id {idx}: {response.text}")
    
    time.sleep(10)
