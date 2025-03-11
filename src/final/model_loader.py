# model/model_loader.py

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# Chemin vers le modèle sauvegardé
MODEL_SAVE_PATH = "saved_models/albert"

def load_model():
    """
    Charge et retourne le modèle ainsi que le tokenizer.
    """
    tokenizer = AutoTokenizer.from_pretrained(MODEL_SAVE_PATH)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_SAVE_PATH)
    return model, tokenizer

def classify_message(model, tokenizer, text):
    """
    Prend en entrée un texte et retourne la classification ('benign' ou 'jailbreak')
    ainsi que le niveau de confiance.
    """
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    # 0 pour benign, 1 pour jailbreak
    prediction = outputs.logits.argmax(dim=-1).item()
    label = "jailbreak" if prediction == 1 else "benign"
    confidence = torch.softmax(outputs.logits, dim=-1)[0][prediction].item()
    return label, confidence

# Bloc de test si on exécute directement ce module
if __name__ == '__main__':
    model, tokenizer = load_model()
    message_text = "This is a benign app"
    label, confidence = classify_message(model, tokenizer, message_text)
    print(f"Text: {message_text}, Classification: {label}, Confidence: {confidence:.4f}")
