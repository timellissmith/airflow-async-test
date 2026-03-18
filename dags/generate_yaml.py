import os

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "api_calls.yaml")

def generate_yaml_string(num_calls=5000):
    lines = []
    for i in range(num_calls):
        lines.append(f"- id: call_{i}")
        lines.append(f"  url: https://mock.api/v1/resource/{i}")
        lines.append(f"  wait: 1")
    return "\n".join(lines)

if __name__ == "__main__":
    num_calls = 5000
    yaml_content = generate_yaml_string(num_calls)
    with open(OUTPUT_FILE, "w") as f:
        f.write(yaml_content)
    print(f"Generated {num_calls} mock API calls to {OUTPUT_FILE}")
