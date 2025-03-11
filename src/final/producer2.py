from flask import Flask, request, jsonify
import pulsar
import json

app = Flask(__name__)

# Initialisation du client Pulsar et création du producteur pour le topic de stockage
client = pulsar.Client('pulsar://localhost:6650')
producer_storage = client.create_producer('persistent://public/default/messages-storage')

@app.route('/send', methods=['POST'])
def send_message():
    # Récupération des données depuis la requête JSON
    data = request.get_json()
    message_id = data.get('id')  # Par exemple, l'index du CSV
    message = data.get('message', '')
    
    # Création d'un payload combinant l'ID et le message
    payload = {'id': message_id, 'message': message}
    payload_str = json.dumps(payload)
    
    # Publication du payload dans le topic de stockage uniquement
    producer_storage.send(payload_str.encode('utf-8'))
    
    return jsonify({'status': 'Message sent', 'id': message_id, 'message': message})

if __name__ == '__main__':
    app.run(port=5000)
