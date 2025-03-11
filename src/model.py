# load the model and tokenizer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import json

def classify_message(model, tokenizer, text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    
    # Get the predicted class (0 for benign, 1 for jailbreak)
    predictions = outputs.logits.argmax(dim=-1).item()
    label = "jailbreak" if predictions == 1 else "benign"
    
    confidence = torch.softmax(outputs.logits, dim=-1)[0][predictions].item()
    return label, confidence


model_save_path = "Jailbreak-Detection/saved_models/albert"
tokenizer = AutoTokenizer.from_pretrained(model_save_path)
model = AutoModelForSequenceClassification.from_pretrained(model_save_path)

message_text = "This is a benign app"

inputs = tokenizer(message_text, return_tensors="pt")

label, confidence = classify_message(model, tokenizer, message_text)
print(f"Classification: {label}, Confidence: {confidence:.4f}")

# Print the result
result = {
    "message": message_text,
    "classification": label,
    "confidence": f"{confidence:.4f}"
}

print(json.dumps(result, indent=2))
            