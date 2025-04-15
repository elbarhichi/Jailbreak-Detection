from datasets import load_dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import TrainingArguments
from transformers import Trainer
import evaluate
import numpy as np
device = "cuda"

dataset = load_dataset("jackhhao/jailbreak-classification")
print(dataset['train'][0])

# Load DeBERTa tokenizer and model
model_name = "microsoft/deberta-v3-base"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=2).to(device)  # 2 labels: jailbreak/benign

def tokenize_function(example):
    return tokenizer(
        example['prompt'],
        truncation=True,
        padding="max_length",
        max_length=512
    )

tokenized_dataset = dataset.map(tokenize_function, batched=True)

training_args = TrainingArguments(
    per_device_train_batch_size=4,   # Batch size per device
    per_device_eval_batch_size=4,
    output_dir="./deberta_results",  # Directory to save checkpoints
    evaluation_strategy="epoch",     # Evaluate after each epoch
    save_strategy="epoch",           # Save model after each epoch
    learning_rate=2e-5,              # Recommended learning rate
    num_train_epochs=3,              # Number of epochs
    weight_decay=0.01,               # Weight decay for regularization
    logging_dir="./deberta_logs",    # Directory for logs
    load_best_model_at_end=True,     # Automatically load best model
    metric_for_best_model="f1",      # Changed to F1 score as it combines precision and recall
)

# Load metrics
accuracy_metric = evaluate.load("accuracy")
precision_metric = evaluate.load("precision")
recall_metric = evaluate.load("recall")
f1_metric = evaluate.load("f1")

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    
    # Compute all metrics
    accuracy = accuracy_metric.compute(predictions=predictions, references=labels)
    precision = precision_metric.compute(predictions=predictions, references=labels, average='binary')
    recall = recall_metric.compute(predictions=predictions, references=labels, average='binary')
    f1 = f1_metric.compute(predictions=predictions, references=labels, average='binary')
    
    # Combine all metrics in a single dictionary
    metrics = {
        'accuracy': accuracy['accuracy'],
        'precision': precision['precision'],
        'recall': recall['recall'],
        'f1': f1['f1']
    }
    
    return metrics

def add_labels(example):
    example['labels'] = 1 if example['type'] == 'jailbreak' else 0
    return example

# Add the 'labels' column to the dataset
tokenized_dataset = tokenized_dataset.map(add_labels)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_dataset['train'],
    eval_dataset=tokenized_dataset['test'],
    tokenizer=tokenizer,
    compute_metrics=compute_metrics
)

trainer.train()

results = trainer.evaluate()
print("\nTest Results:")
print(f"Accuracy: {results['eval_accuracy']:.4f}")
print(f"Precision: {results['eval_precision']:.4f}")
print(f"Recall: {results['eval_recall']:.4f}")
print(f"F1 Score: {results['eval_f1']:.4f}")