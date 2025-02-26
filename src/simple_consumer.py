

import json
import time

import torch


from transformers import AutoTokenizer, AutoModelForSequenceClassification
import pulsar
# Model loading function
def load_jailbreak_classifier():
    model_path = "./deberta_results/checkpoint-783"  # Path to your saved model
    tokenizer = AutoTokenizer.from_pretrained("microsoft/deberta-v3-base")
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    model.eval()  # Set to evaluation mode
    return model, tokenizer
# Classification function
def classify_message(model, tokenizer, text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    
    # Get the predicted class (0 for benign, 1 for jailbreak)
    predictions = outputs.logits.argmax(dim=-1).item()
    label = "jailbreak" if predictions == 1 else "benign"
    
    confidence = torch.softmax(outputs.logits, dim=-1)[0][predictions].item()
    return label, confidence


def simple_consumer():
    try:
        # Création du client
        client = pulsar.Client('pulsar://localhost:6650')
        
        # Création du consumer
        consumer = client.subscribe('test-topic', subscription_name='my-subscription')
        
        print("Consumer démarré. En attente de messages...")

        while True:
            msg = consumer.receive()
            print(f"Reçu: {msg.data().decode('utf-8')}")
            consumer.acknowledge(msg)

    except Exception as e:
        print(f"Erreur dans le consumer: {e}")
    finally:
        client.close()


def less_simple_consumer():


    model, tokenizer = load_jailbreak_classifier()

    # Connect to Pulsar
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('test-topic', 'my-subscription')

    print("Consumer started. Waiting for messages...")

    try:
        while True:
            # Wait for a message
            msg = consumer.receive()
            
            # Process the message
            message_text = msg.data().decode('utf-8')
            print(f"Received: {message_text}")
            label, confidence = classify_message(model, tokenizer, message_text)
            print(f"Classification: {label}, Confidence: {confidence:.4f}")
            # Acknowledge the message
            consumer.acknowledge(msg)
            
            # Print the result
            result = {
                "message": message_text,
                "classification": label,
                "confidence": f"{confidence:.4f}"
            }
            print(json.dumps(result, indent=2))
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close connections
        client.close()

if __name__ == "__main__":
    less_simple_consumer()
