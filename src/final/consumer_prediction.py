import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pulsar
from pulsar import Timeout, InitialPosition
import json
from model_loader import load_model, classify_message

# DoFn pour lire depuis le topic "messages-storage"
class ReadStorageFromPulsarDoFn(beam.DoFn):
    def setup(self):
        self.client = pulsar.Client('pulsar://localhost:6650')
        # Souscription dédiée pour le pipeline de prédiction, en lisant depuis le début
        self.consumer = self.client.subscribe(
            'persistent://public/default/messages-storage',
            subscription_name='beam-storage-prediction',
            initial_position=InitialPosition.Earliest
        )

    def process(self, element, *args, **kwargs):
        try:
            msg = self.consumer.receive(timeout_millis=500)
            if msg:
                self.consumer.acknowledge(msg)
                yield msg.data().decode('utf-8')
        except Timeout:
            return

    def teardown(self):
        self.client.close()

class ReadStorageFromPulsar(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(ReadStorageFromPulsarDoFn())

# DoFn pour appliquer le modèle de prédiction
class PredictDoFn(beam.DoFn):
    def setup(self):
        self.model, self.tokenizer = load_model()

    def process(self, element):
        # On suppose que l'élément est une chaîne JSON contenant "id" et "message"
        data = json.loads(element)
        label, confidence = classify_message(self.model, self.tokenizer, data['message'])
        result = {
            "id": data['id'],
            "message": data['message'],
            "classification": label,
            "confidence": f"{confidence:.4f}"
        }
        # On renvoie le résultat sous forme de JSON
        yield json.dumps(result)

# DoFn pour publier les résultats de prédiction dans le topic "predictions"
class PublishResultsDoFn(beam.DoFn):
    def setup(self):
        self.client = pulsar.Client('pulsar://localhost:6650')
        self.producer = self.client.create_producer('persistent://public/default/predictions')

    def process(self, element):
        self.producer.send(element.encode('utf-8'))
        # Optionnel : renvoyer l'élément pour suivi
        yield element

    def teardown(self):
        self.client.close()

def tick_generator():
    # Génère une séquence d'entiers pour déclencher périodiquement la lecture
    for i in range(1000):
        yield i

def run():
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        # Utilisation de ticks pour déclencher périodiquement la lecture depuis Pulsar
        ticks = pipeline | "Créer ticks" >> beam.Create(list(tick_generator()))
        
        # Lecture des messages depuis "messages-storage"
        messages = ticks | "Lire Storage" >> ReadStorageFromPulsar()
        
        # Application du modèle pour obtenir la prédiction
        predictions = messages | "Appliquer le modèle" >> beam.ParDo(PredictDoFn())
        
        # Publication des résultats dans le topic "predictions"
        published = predictions | "Publier Résultats" >> beam.ParDo(PublishResultsDoFn())
        
        # Optionnel : affichage dans la console
        published | "Afficher Résultats" >> beam.Map(print)

if __name__ == '__main__':
    run()
