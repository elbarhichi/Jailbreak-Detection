import pulsar

# Créez un client Pulsar qui se connecte au broker
client = pulsar.Client('pulsar://localhost:6650')

# Souscrivez au topic "chatbot-inference" dans le namespace public/default
# "my-subscription" est le nom de la souscription ; vous pouvez le changer ou en créer un nouveau pour lire depuis le début.
consumer = client.subscribe('persistent://public/default/chatbot-inference', 'my-subscription')

print("Le consumer est en attente de messages...")

# Boucle infinie pour recevoir les messages
try:
    while True:
        # Bloque jusqu'à ce qu'un message soit disponible
        msg = consumer.receive()
        try:
            # Affiche le contenu du message (décodé en UTF-8)
            print("Message reçu :", msg.data().decode('utf-8'))
            # Confirme la réception du message pour éviter sa redélivrance
            
            consumer.acknowledge(msg)
        except Exception as e:
            # En cas d'erreur, indique au broker de redélivrer ce message
            consumer.negative_acknowledge(msg)
except KeyboardInterrupt:
    print("Arrêt du consumer...")

# Ferme proprement le client
client.close()
