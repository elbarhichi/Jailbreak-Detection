import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pulsar
from pulsar import Timeout, InitialPosition
import json
import os

host = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")

# DoFn pour lire depuis le topic "messages-storage"
class ReadStorageForJoinDoFn(beam.DoFn):
    def setup(self):
        self.client = pulsar.Client(host)
        # Souscription dédiée pour la jointure, en lisant tous les messages existants
        self.consumer = self.client.subscribe(
            'persistent://public/default/messages-storage',
            subscription_name='beam-storage-join',
            initial_position=InitialPosition.Earliest
        )
        print("ReadStorageForJoinDoFn setup completed.")

    def process(self, element, *args, **kwargs):
        try:
            msg = self.consumer.receive(timeout_millis=500)
            if msg:
                self.consumer.acknowledge(msg)
                data = json.loads(msg.data().decode('utf-8'))
                print(f"Message Storage reçu: {data}")
                yield (data['id'], data['message'])
            else:
                print("Aucun message Storage reçu sur ce tick.")
        except Timeout:
            return
 

    def teardown(self):
        self.client.close()

class ReadStorageForJoin(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(ReadStorageForJoinDoFn())

# DoFn pour lire depuis le topic "predictions"
class ReadPredictionsForJoinDoFn(beam.DoFn):
    def setup(self):
        self.client = pulsar.Client(host)
        # Souscription dédiée pour les prédictions, en lisant tous les messages
        self.consumer = self.client.subscribe(
            'persistent://public/default/predictions',
            subscription_name='beam-predictions',
            initial_position=InitialPosition.Earliest
        )
        print("ReadPredictionsForJoinDoFn setup completed.")

    def process(self, element, *args, **kwargs):
        try:
            msg = self.consumer.receive(timeout_millis=500)
            if msg:
                self.consumer.acknowledge(msg)
                data = json.loads(msg.data().decode('utf-8'))
                print(f"Message Prediction reçu: {data}")
                # On émet un tuple (id, résultat de prédiction)
                prediction = {
                    'classification': data.get('classification'),
                    'confidence': data.get('confidence')
                }
                yield (data['id'], prediction)
            else:
                print("Aucun message Prediction reçu sur ce tick.")
        except Timeout:
            return

    def teardown(self):
        self.client.close()

class ReadPredictionsForJoin(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(ReadPredictionsForJoinDoFn())

# Générateur de ticks pour déclencher périodiquement la lecture
def tick_generator(n):
    for i in range(n):
        yield i

def run():
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        # Création de deux sources de ticks pour déclencher la lecture
        ticks_storage = pipeline | "TickStorage" >> beam.Create(list(tick_generator(1000)))
        ticks_predictions = pipeline | "TickPredictions" >> beam.Create(list(tick_generator(1000)))
        
        # Lecture des messages du topic "messages-storage"
        storage_pc = ticks_storage | "Lire Storage" >> ReadStorageForJoin()
        # Lecture des messages du topic "predictions"
        predictions_pc = ticks_predictions | "Lire Predictions" >> ReadPredictionsForJoin()
        
        # Jointure des deux PCollections par ID
        joined = ({
            'storage': storage_pc,
            'predictions': predictions_pc
        } | "Jointure par ID" >> beam.CoGroupByKey())
        
        # Assemblage des résultats de la jointure
        def process_join(element):
            key, grouped = element
            storage_messages = grouped['storage']
            predictions_list = grouped['classification']
            result = {
                'id': key,
                'message': storage_messages[0] if storage_messages else None,
                'prediction': predictions_list[0] if predictions_list else None
            }
            print(f"Jointure pour ID {key}: {result}")
            return result
        
        results = joined | "Assembler Jointure" >> beam.Map(process_join)
        
        # Affichage final des résultats
        results | "Afficher Résultats" >> beam.Map(print)

if __name__ == '__main__':
    run()
