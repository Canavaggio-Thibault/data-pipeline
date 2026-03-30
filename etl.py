import csv

def extract(file_path):
    """Extract data from a CSV file."""
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def validate(data):
    """Validate data by checking for required fields and valid values."""
    validated_data = []
    for row in data:
        # Check if required fields exist
        if 'name' not in row or not row['name']:
            continue  # Skip rows without a name
        if 'age' not in row:
            continue  # Skip rows without age
        try:
            age = int(row['age'])
            if age < 0:
                continue  # Skip negative age
        except ValueError:
            continue  # Skip rows with non-integer age
        # If we get here, the row is valid
        validated_data.append(row)
    return validated_data

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
    validated = validate(extracted)
    transformed = transform(validated)
    load(transformed, 'output.csv')