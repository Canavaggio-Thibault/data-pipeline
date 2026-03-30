import csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract(file_path):
    """Extract data from a CSV file."""
    data = []
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        logger.info(f"Successfully extracted {len(data)} rows from {file_path}")
    except FileNotFoundError:
        logger.error(f"The file {file_path} was not found.")
        return []
    except Exception as e:
        logger.error(f"An error occurred while reading the file: {e}")
        return []
    return data

def validate(data):
    """Validate data by checking for required fields and valid values."""
    validated_data = []
    for row in data:
        # Check if required fields exist
        if 'name' not in row or not row['name']:
            logger.warning(f"Skipping row without name: {row}")
            continue  # Skip rows without a name
        if 'age' not in row:
            logger.warning(f"Skipping row without age: {row}")
            continue  # Skip rows without age
        try:
            age = int(row['age'])
            if age < 0:
                logger.warning(f"Skipping row with negative age: {row}")
                continue  # Skip negative age
        except ValueError:
            logger.warning(f"Skipping row with non-integer age: {row}")
            continue  # Skip rows with non-integer age
        # If we get here, the row is valid
        validated_data.append(row)
    logger.info(f"Validated {len(validated_data)} rows out of {len(data)}")
    return validated_data

def transform(data):
    """Transform data by converting age to integer and adding a new field."""
    transformed_count = 0
    skipped_count = 0
    for row in data:
        try:
            row['age'] = int(row['age'])
            row['is_adult'] = row['age'] >= 18
            transformed_count += 1
        except ValueError:
            # If age conversion fails, skip the row
            logger.warning(f"Skipping row due to invalid age conversion: {row}")
            skipped_count += 1
            continue
    logger.info(f"Transformed {transformed_count} rows, skipped {skipped_count} rows")
    return data

def load(data, file_path):
    """Load data to a CSV file."""
    if not data:
        logger.warning("No data to load.")
        return
    try:
        fieldnames = data[0].keys()
        with open(file_path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"Successfully loaded {len(data)} rows to {file_path}")
    except Exception as e:
        logger.error(f"An error occurred while writing the file: {e}")

if __name__ == "__main__":
    # Example usage
    logger.info("Starting ETL process")
    extracted = extract('data.csv')
    validated = validate(extracted)
    transformed = transform(validated)
    load(transformed, 'output.csv')
    logger.info("ETL process completed")