import yaml
import json

def yaml_to_ndjson(input_file, output_file):
    with open(input_file, 'r') as f:
        data = yaml.safe_load(f)
    
    with open(output_file, 'w') as f:
        for entry in data:
            f.write(json.dumps(entry) + '\n')
    print(f"Converted {len(data)} entries to {output_file}")

if __name__ == "__main__":
    yaml_to_ndjson("dags/api_calls.yaml", "dags/api_calls.ndjson")
