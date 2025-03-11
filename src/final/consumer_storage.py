# consumer_storage.py

import pulsar
import json

def main():
    # Connexion au broker Pulsar
    client = pulsar.Client('pulsar://localhost:6650')
    # Souscription au topic "messages-storage"
    consumer = client.subscribe(
        'persistent://public/default/messages-storage',
        subscription_name='sub-storage'
    )
    
    print("Consumer pour le stockage en attente de messages...")
    
    try:
        while True:
            msg = consumer.receive()
            try:
                data = json.loads(msg.data().decode('utf-8'))
                print(f"[Stockage] Message reçu - ID: {data['id']} | Texte: {data['message']}")
                consumer.acknowledge(msg)
            except Exception as e:
                consumer.negative_acknowledge(msg)
                print(f"Erreur de traitement dans consumer_storage: {e}")
    except KeyboardInterrupt:
        print("Arrêt du consumer pour le stockage...")
    finally:
        client.close()

if __name__ == '__main__':
    main()
