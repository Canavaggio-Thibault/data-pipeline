import csv

def extract(file_path):
    """Extract data from a CSV file."""
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def transform(data):
    """Transform data by converting age to integer and adding a new field."""
    for row in data:
        row['age'] = int(row['age'])
        row['is_adult'] = row['age'] >= 18
    return data

def load(data, file_path):
    """Load data to a CSV file."""
    if not data:
        return
    fieldnames = data[0].keys()
    with open(file_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    # Example usage
    extracted = extract('data.csv')
    transformed = transform(extracted)
    load(transformed, 'output.csv')