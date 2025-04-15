import torch
import os
import json
from transformers import AlbertForSequenceClassification, AlbertTokenizer
from apache_beam.ml.inference.base import ModelHandler
from typing import Dict, Any
from apache_beam.ml.inference import RunInference
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pulsar
from pulsar import Timeout, InitialPosition
def tick_generator():
    # Génère une séquence d'entiers pour déclencher périodiquement la lecture
    for i in range(1000):
        yield i

host = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")

class AlbertModelHandler(ModelHandler):
    def __init__(self, model_path: str, device: str = 'cpu'):
        self.model_path = model_path
        self.device = device
        self.model = None
        self.tokenizer = None
        self.load_model()

    def load_model(self):
        """Load or reload the ALBERT model and tokenizer."""
        self.tokenizer = AlbertTokenizer.from_pretrained(self.model_path)
        self.model = AlbertForSequenceClassification.from_pretrained(self.model_path)
        self.model.to(self.device)
        self.model.eval()

    def update_model_path(self, model_path: str):
        """Update the model path and reload the model."""
        self.model_path = model_path
        self.load_model()

    def run_inference(self, inputs) -> Any:
        """Run inference on the input data."""
        data = json.loads(inputs)
        # Tokenize the input text
        inputs = self.tokenizer(data['message'], return_tensors='pt', padding=True, truncation=True).to(self.device)
        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            prediction = torch.argmax(logits, dim=-1)
            label = "jailbreak" if prediction == 1 else "benign"
            confidence = torch.softmax(outputs.logits, dim=-1)[0][prediction].item()
        result = {
            "id": data['id'],
            "classification": label,
            "confidence": f"{confidence:.4f}"
        }
        # On renvoie le résultat sous forme de JSON
        yield json.dumps(result)

# DoFn pour lire depuis le topic "messages-storage"
class ReadStorageFromPulsarDoFn(beam.DoFn):
    def setup(self):
        self.client = pulsar.Client(host)
        # Souscription dédiée pour le pipeline de prédiction, en lisant depuis le début
        self.consumer = self.client.subscribe(
            'persistent://public/default/messages-storage',
            subscription_name='beam-storage-prediction',
            # initial_position=InitialPosition.Earliest
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
# DoFn pour publier les résultats de prédiction dans le topic "predictions"
class PublishResultsDoFn(beam.DoFn):
    def setup(self):
        self.client = pulsar.Client(host)
        self.producer = self.client.create_producer('persistent://public/default/predictions')

    def process(self, element):
        self.producer.send(element.encode('utf-8'))
        # Optionnel : renvoyer l'élément pour suivi
        yield element

    def teardown(self):
        self.client.close()

def run():
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        # Initialize the model handler
        model_path = 'model/albert_production'
        model_handler = AlbertModelHandler(model_path=model_path, device='cuda' if torch.cuda.is_available() else 'cpu')

        # Define the pipeline steps
        (
            pipeline
            | "Créer ticks" >> beam.Create(list(tick_generator()))
            | "Lire Storage" >> ReadStorageFromPulsar()
            | "RunInference" >> beam.ParDo(RunInference(model_handler))
            | "Publier Résultats" >> beam.ParDo(PublishResultsDoFn())
            | "ProcessResults" >> beam.Map(print)
        )

if __name__ == '__main__':
    run()
