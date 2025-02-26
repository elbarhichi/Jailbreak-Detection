import requests
import json
import time

# URL de votre gateway Flask
url = "http://localhost:5000/send"

# Liste de messages à envoyer
messages = [
    "Message 111",
    "Message 22",
    "Message 33",
    "Message 4",
    "Message 5"
]

for msg in messages:

    payload = {"message": msg}
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    
    if response.status_code == 200:
        print(f"Message '{msg}' envoyé avec succès.")
    else:
        print(f"Erreur lors de l'envoi du message '{msg}': {response.text}")
    
    time.sleep(5)
