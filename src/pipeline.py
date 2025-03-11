# beam_pipeline/pipeline.py

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pulsar
from pulsar import Timeout
from model_loader import load_model, classify_message

class ReadFromPulsarDoFn(beam.DoFn):
    def setup(self):
        # Initialisation du client et du consumer Pulsar
        self.client = pulsar.Client('pulsar://localhost:6650')
        # On s'abonne au topic avec une souscription dédiée pour Beam
        self.consumer = self.client.subscribe(
            'persistent://public/default/chatbot-inference',
            subscription_name='beam-subscription'
        )

    def process(self, element, *args, **kwargs):
        """
        Chaque élément (un tick) déclenche une tentative de lecture.
        On utilise un timeout pour éviter de bloquer indéfiniment.
        """
        try:
            # Tenter de recevoir un message avec un timeout de 500ms
            msg = self.consumer.receive(timeout_millis=500)
            # Si un message est reçu, on l'acknowledge et on le renvoie
            if msg:
                self.consumer.acknowledge(msg)
                yield msg.data().decode('utf-8')
        except Timeout:
            # Aucun message reçu durant le timeout, on ne renvoie rien
            return

    def teardown(self):
        self.client.close()

class ReadFromPulsar(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(ReadFromPulsarDoFn())

class PredictDoFn(beam.DoFn):
    def setup(self):
        # Charger le modèle une seule fois par worker
        self.model, self.tokenizer = load_model()

    def process(self, element):
        # Appliquer la classification sur le message
        label, confidence = classify_message(self.model, self.tokenizer, element)
        result = {
            "message": element,
            "classification": label,
            "confidence": f"{confidence:.4f}"
        }
        yield result

def tick_generator():
    # Génère une séquence d'entiers de 0 à 999
    for i in range(1000):
        yield i



def run():
    # Options pour indiquer un pipeline en mode streaming (si nécessaire)
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        # Pour déclencher la lecture de Pulsar, on crée une source de ticks.
        # Ici, on génère une séquence d'entiers pour simuler un déclencheur périodique.
        # ticks = pipeline | "Créer ticks" >> beam.io.GenerateSequence(start=0, stop=1000)
        ticks = pipeline | "Créer ticks" >> beam.Create(list(tick_generator()))

        # Lire les messages de Pulsar à partir des ticks
        messages = ticks | "Lire depuis Pulsar" >> ReadFromPulsar()

        # Appliquer la prédiction sur chaque message
        predictions = messages | "Appliquer le modèle" >> beam.ParDo(PredictDoFn())

        # Pour l'exemple, on affiche les résultats dans la console
        predictions | "Afficher les résultats" >> beam.Map(print)

if __name__ == '__main__':
    run()
