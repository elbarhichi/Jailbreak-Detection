from flask import Flask, request, jsonify
import pulsar

app = Flask(__name__)

# Initialisation du client Pulsar et création d'un producteur sur le topic désiré
client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('persistent://public/default/chatbot-inference')

@app.route('/send', methods=['POST'])
def send_message():
    # Récupération du message depuis la requête JSON
    data = request.get_json()
    message = data.get('message', '')
    
    # Publication du message sur le topic
    producer.send(message.encode('utf-8'))
    
    return jsonify({'status': 'Message sent', 'message': message})

if __name__ == '__main__':
    app.run(port=5000)
