import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F
from datasets import load_dataset

# Define model and tokenizer paths
model_name = "microsoft/deberta-v3-base"
checkpoint_dir = "./deberta_results/checkpoint-783"  # Path to the directory where checkpoints are saved

# Load the tokenizer and the model from the latest checkpoint
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(checkpoint_dir).to("cuda")
model.eval()  # Set the model to evaluation mode

# Load the dataset
dataset = load_dataset("jackhhao/jailbreak-classification")
test_data = dataset["test"]

# Pick a sentence from the test dataset
example = test_data[0]
sentence = example["prompt"]
print(f"Input Sentence: {sentence}")
# Tokenize the input sentence
inputs = tokenizer(sentence, truncation=True, padding="max_length", max_length=512, return_tensors="pt").to("cuda")

# Perform inference
with torch.no_grad():
    logits = model(**inputs).logits
    probabilities = F.softmax(logits, dim=-1).squeeze().cpu().numpy()

# Print the softmaxed output
print(f"Softmaxed Output: {probabilities}")
print(f"Predicted Class: {'Jailbreak' if probabilities[1] > probabilities[0] else 'Benign'}")
