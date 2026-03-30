import csv

def extract(file_path):
    """Extract data from a CSV file."""
    data = []
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return []
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        return []
    return data

def transform(data):
    """Transform data by converting age to integer and adding a new field."""
    for row in data:
        try:
            row['age'] = int(row['age'])
            row['is_adult'] = row['age'] >= 18
        except ValueError:
            # If age conversion fails, skip the row
            continue
    return data

def load(data, file_path):
    """Load data to a CSV file."""
    if not data:
        print("Warning: No data to load.")
        return
    try:
        fieldnames = data[0].keys()
        with open(file_path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        print(f"An error occurred while writing the file: {e}")

if __name__ == "__main__":
    # Example usage
    extracted = extract('data.csv')
    transformed = transform(extracted)
    load(transformed, 'output.csv')