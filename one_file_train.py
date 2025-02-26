from datasets import load_dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import TrainingArguments
from transformers import Trainer
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
#print(tokenized_dataset['train'][0])  # Should include input_ids and attention_mask



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
    metric_for_best_model="accuracy",
)

import evaluate

# Load the accuracy metric
metric = evaluate.load("accuracy")

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = logits.argmax(axis=-1)  # Get the class with the highest probability
    return metric.compute(predictions=predictions, references=labels)

def add_labels(example):
    example['labels'] = 1 if example['type'] == 'jailbreak' else 0
    return example

# Add the 'labels' column to the dataset
tokenized_dataset = tokenized_dataset.map(add_labels)

def add_labels(example):
    example['labels'] = 1 if example['type'] == 'jailbreak' else 0
    return example


trainer = Trainer(
    model=model,
    args=training_args,  # TrainingArguments defined earlier
    train_dataset=tokenized_dataset['train'],
    eval_dataset=tokenized_dataset['test'],
    tokenizer=tokenizer,
    compute_metrics=compute_metrics  # Add this line
)



# Add the 'labels' column to the dataset
tokenized_dataset = tokenized_dataset.map(add_labels)
trainer.train()

results = trainer.evaluate()
print("Test Results:", results)