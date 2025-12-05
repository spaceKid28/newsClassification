from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers.onnx import FeaturesManager, export
from pathlib import Path

model_id = "textattack/roberta-base-MNLI"
feature = "sequence-classification"
opset = 13

output_dir = Path("./scala_project/src/main/resources/onnx-model")
output_dir.mkdir(parents=True, exist_ok=True)

# Load tokenizer & model
tokenizer = AutoTokenizer.from_pretrained(model_id)
model = AutoModelForSequenceClassification.from_pretrained(model_id)

# Save tokenizer files
tokenizer.save_pretrained(output_dir)

# Correct ONNX config for sequence classification
onnx_config = FeaturesManager.get_config(model.config, feature)

onnx_path = output_dir / "model.onnx"

# Export ONNX
export(
    model=model,
    tokenizer=tokenizer,
    config=onnx_config,
    opset=opset,
    output=onnx_path
)

print("ONNX model exported to:", onnx_path)