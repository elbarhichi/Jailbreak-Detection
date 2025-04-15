import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.ml.inference.base import RunInference, ModelHandler
# Import PredictionResult if needed, or handle output differently if RunInference yields it directly
# from apache_beam.ml.inference.base import PredictionResult
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.transforms.trigger import AfterProcessingTime, Repeatedly
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms import DoFn, ParDo, PTransform, WindowInto
import os
import json
import torch
import logging
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import pulsar
from pulsar import Timeout
import time # Added for timestamping

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define a Key type for RunInference if needed, or adapt based on ModelHandler's expected input
# For simplicity, let's assume the input string itself is the key and value for RunInference
# Or define a simple key like its id if available in the raw message.


class AlbertModelHandler(ModelHandler[str, str, AutoModelForSequenceClassification]):
    def __init__(self, model_path: str, device: str = 'cpu'):
        self.model_path = model_path
        self.device = device
        self._model = None
        self._tokenizer = None
        self._model_load_time = 0 # Track when the model was loaded
        self._check_interval = 30 # Check for model updates every 30 seconds
        self.load_model() # Initial load

    def load_model(self) -> AutoModelForSequenceClassification:
        """Load or reload the ALBERT model and tokenizer."""
        current_time = time.time()
        # Throttle checks to avoid excessive disk I/O
        if current_time - self._model_load_time < self._check_interval and self._model is not None:
             return self._model

        try:
            if not os.path.exists(self.model_path):
                if self._model is None: # Only raise error if model was never loaded
                     raise FileNotFoundError(f"Model path {self.model_path} does not exist.")
                else:
                     logging.warning(f"Model path {self.model_path} no longer exists. Using cached model.")
                     self._model_load_time = current_time # Update time to throttle next check
                     return self._model # Return the cached model

            latest_mtime = max(
                (os.path.getmtime(os.path.join(self.model_path, f))
                 for f in os.listdir(self.model_path) if os.path.isfile(os.path.join(self.model_path, f))), # check only files
                default=0 # Default to 0 if directory is empty or doesn't exist
            )

            # If model exists and hasn't been loaded yet, or if files have been modified
            if self._model is None or latest_mtime > self._model_load_time:
                logging.info(f"Loading/Reloading model from {self.model_path} (updated at {time.ctime(latest_mtime)})")
                self._tokenizer = AutoTokenizer.from_pretrained(self.model_path)
                self._model = AutoModelForSequenceClassification.from_pretrained(self.model_path)
                self._model.to(self.device)
                self._model.eval()
                self._model_load_time = latest_mtime # Use file mtime for future comparisons
                logging.info(f"Model successfully loaded/reloaded onto device {self.device}.")
            else:
                # Update check time even if no reload happened
                #self._model_load_time = current_time
                pass

        except Exception as e:
            logging.error(f"Error loading model from {self.model_path}: {e}")
            # If a model was previously loaded, keep using it. Otherwise, raise to fail the pipeline.
            if self._model is None:
                raise e # Fail if initial load fails
            else:
                logging.warning("Continuing with previously loaded model due to error.")
                #self._model_load_time = current_time # Update time to throttle next check

        return self._model # Return the potentially updated or cached model


    def run_inference(self, batch, model, inference_args=None):
        """Run inference on a batch of input data."""
        # Model is loaded 'on-demand' by the base RunInference transform which calls load_model()
        # The `model` parameter here is the result of `load_model()`

        results = []
        texts_to_process = []
        original_data = []
        self.load_model() 
        # 1. Preprocessing
        for element in batch:
            try:
                # Assuming element is the raw JSON string from Pulsar
                data = json.loads(element)
                texts_to_process.append(data['message'])
                original_data.append(data) # Keep original data to include id later
            except (json.JSONDecodeError, KeyError) as e:
                logging.warning(f"Skipping invalid input: {element}. Error: {e}")
                # Append a placeholder or handle error appropriately
                results.append(json.dumps({"error": "Invalid input format", "input": element})) # Yield error back

        if not texts_to_process:
             return [] # Return empty list if no valid data in batch

        # 2. Tokenization and Inference
        try:
            inputs = self._tokenizer(texts_to_process, return_tensors='pt', padding=True, truncation=True).to(self.device)
            with torch.no_grad():
                outputs = model(**inputs)
                logits = outputs.logits
                predictions = torch.argmax(logits, dim=-1)
                confidences = torch.softmax(logits, dim=-1)

            # 3. Postprocessing
            for i, prediction in enumerate(predictions):
                pred_label = "jailbreak" if prediction.item() == 1 else "benign"
                confidence = confidences[i][prediction].item()
                result = {
                    "id": original_data[i].get('id', 'unknown'), # Use get for safety
                    "classification": pred_label,
                    "confidence": f"{confidence:.4f}"
                }
                latest_mtime = max(
                    (os.path.getmtime(os.path.join(self.model_path, f))
                    for f in os.listdir(self.model_path) if os.path.isfile(os.path.join(self.model_path, f))), # check only files
                    default=0 # Default to 0 if directory is empty or doesn't exist
                )
                logging.info(f"{time.ctime(self._model_load_time)}_{time.ctime(latest_mtime)}_{result}")
                results.append(json.dumps(result))

        except Exception as e:
            logging.error(f"Error during batch inference: {e}")
            # Add error messages for the entire batch or handle individually
            for element in batch:
                 results.append(json.dumps({"error": "Inference failed", "input": element}))

        return results

    # Optional: Implement inference_args if needed
    # def get_inference_arguments(self):
    #     return {'some_arg': 'value'}

class ReadStorageFromPulsarDoFn(DoFn):
    # Keep this DoFn as is, but consider error handling and potential blocking
    def setup(self):
        pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
        logging.info(f"Connecting to Pulsar at {pulsar_url}")
        try:
            self.client = pulsar.Client(pulsar_url)
            self.consumer = self.client.subscribe(
                'persistent://public/default/messages-storage',
                subscription_name='beam-storage-prediction-sub', # Use a descriptive subscription name
                consumer_type=pulsar.ConsumerType.Shared, # Consider Shared if multiple workers
                initial_position=pulsar.InitialPosition.Earliest # Or Latest depending on need
            )
            logging.info("Pulsar consumer connected.")
        except Exception as e:
            logging.error(f"Failed to connect Pulsar consumer: {e}")
            raise # Fail fast if connection fails

    def process(self, element, *args, **kwargs):
        # This DoFn now acts as a generator when used with beam.Impulse >> beam.FlatMap
        # It will continuously try to receive messages.
        try:
            # Using a longer timeout or no timeout might be better for streaming source
            # but requires careful handling of pipeline draining. 500ms is short.
            msg = self.consumer.receive(timeout_millis=1000) # Increased timeout slightly
            if msg:
                try:
                    data = msg.data().decode('utf-8')
                    self.consumer.acknowledge(msg)
                    yield data
                except Exception as e:
                    logging.error(f"Error processing received message: {e}. Nacking.")
                    self.consumer.negative_acknowledge(msg) # Nack bad messages
            else:
                # No message received within timeout, yield control briefly
                # This prevents a tight loop consuming CPU if Pulsar is empty
                time.sleep(0.1) # Small sleep
                #logging.info("No message received. Sleeping briefly.")
                # To make this finite for testing/draining, you might add a counter or time limit

        except Timeout:
            # Timeout is expected, just continue loop
            # logging.debug("Pulsar receive timeout.") # Optional: Debug log
            time.sleep(0.1) # Small sleep on timeout too
            #logging.info("No message received within timeout. Sleeping briefly.")
            return
        except Exception as e:
            logging.error(f"Error receiving from Pulsar: {e}")
            time.sleep(1) # Wait a bit longer after a receive error
            return

    def teardown(self):
        logging.info("Closing Pulsar consumer and client.")
        if hasattr(self, 'consumer') and self.consumer:
            self.consumer.close()
        if hasattr(self, 'client') and self.client:
            self.client.close()
        logging.info("Pulsar resources closed.")

# PTransform wrapper is good practice but not strictly necessary here
# class ReadStorageFromPulsar(PTransform):
#     def expand(self, pcoll):
#         # Using FlatMap allows the DoFn to yield multiple messages over time
#         return pcoll | beam.FlatMap(ReadStorageFromPulsarDoFn())

class PublishResultsDoFn(DoFn):
    # Keep this DoFn as is
    def setup(self):
        pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
        logging.info(f"Connecting Pulsar producer to {pulsar_url}")
        try:
            self.client = pulsar.Client(pulsar_url)
            self.producer = self.client.create_producer('persistent://public/default/predictions')
            logging.info("Pulsar producer connected.")
        except Exception as e:
            logging.error(f"Failed to connect Pulsar producer: {e}")
            raise

    def process(self, element):
        try:
            # Element should already be a JSON string from run_inference
            self.producer.send(element.encode('utf-8'))
            # logging.info(f"Published prediction: {element}") # Optional: log published data
            yield element # Yield element downstream if needed
        except Exception as e:
            logging.error(f"Failed to publish message to Pulsar: {e}")
            # Decide how to handle publish failures (e.g., retry, log, discard)

    def teardown(self):
        logging.info("Closing Pulsar producer and client.")
        if hasattr(self, 'producer') and self.producer:
            self.producer.close()
        if hasattr(self, 'client') and self.client:
            self.client.close()
        logging.info("Pulsar producer resources closed.")

def tick_generator(n):
    for i in range(n):
        yield i

def run():
    print("Starting Beam pipeline...", flush=True)
    pipeline_options = PipelineOptions(
        streaming=True,
        # Add other options like runner, project, region, job_name as needed
        # e.g., runner='DataflowRunner', project='your-gcp-project', region='us-central1'
        # save_main_session=True # Often needed for Dataflow to pickle dependencies correctly
    )
    model_dir = 'model/albert'
    for file in os.listdir():
        logging.info(f"found file : {file}")
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logging.info(f"Using device: {device}")

    # Instantiate the ModelHandler - RunInference will manage its lifecycle per worker
    # The ModelHandler's load_model will be called by RunInference internally.
    # The check for updates is now *inside* the load_model method.
    model_handler = AlbertModelHandler(model_path=model_dir, device=device)

    with beam.Pipeline(options=pipeline_options) as pipeline:

        # Use Impulse for a continuous streaming source trigger
        messages = (
            pipeline
            | "Impulsion initiale" >> beam.Create(list(tick_generator(1000))) # Déclencheur unique
            # Utilisez FlatMap pour permettre à ReadStorageFromPulsarDoFn de générer des messages en continu
            | "ReadFromPulsarContinuously" >> beam.ParDo(ReadStorageFromPulsarDoFn())
            # La PCollection 'messages' contiendra maintenant les strings JSON lues depuis Pulsar
        )



        # Run inference using the RunInference transform directly
        predictions = (
            messages
            # RunInference expects KV pairs by default, but can handle single elements
            # if the ModelHandler is adapted or if you map elements to KVs.
            # Let's assume AlbertModelHandler handles raw strings directly in its run_inference
            | 'RunInference' >> RunInference(model_handler)
            # The output of RunInference is typically PredictionResult(example, inference)
            # We need to extract the inference result (which is JSON string in our case)
            # Note: The exact output type depends on how run_inference returns values.
            # If run_inference returns the JSON string directly as shown in the handler above:
            # | 'ExtractPrediction' >> beam.Map(lambda x: x) # If output is already the string
            # If run_inference follows the standard pattern and returns PredictionResult:
            # | 'ExtractPrediction' >> beam.Map(lambda x: x.inference) # Assuming x is PredictionResult(example, inference)
        )

        # Publish results to Pulsar
        _ = ( # Use _ if the final result isn't used further in the pipeline definition
             predictions
             | 'PublishResults' >> ParDo(PublishResultsDoFn())
        )

        logging.info("Pipeline constructed.")
        # The pipeline runs when the 'with' block exits for direct runner,
        # or after submission for remote runners.

if __name__ == '__main__':
    logging.info("Script starting.")
    try:
        run()
        logging.info("Pipeline execution initiated (may run indefinitely for streaming).")
    except Exception as e:
        logging.exception(f"Pipeline failed to run: {e}") # Log full traceback