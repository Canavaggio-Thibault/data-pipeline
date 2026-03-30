import csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract(file_path):
    """Extract data from a CSV file.
    Yields fieldnames first, then each row as a dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                logger.error(f"CSV file {file_path} has no header or is empty.")
                return
            yield fieldnames
            for row in reader:
                yield row
    except FileNotFoundError:
        logger.error(f"The file {file_path} was not found.")
        return
    except Exception as e:
        logger.error(f"An error occurred while reading the file: {e}")
        return

def validate_row(row):
    """Validate a single row by checking for required fields and valid values.
    Returns the row if valid, None otherwise.
    """
    # Check if required fields exist
    if 'name' not in row or not row['name']:
        logger.warning(f"Skipping row without name: {row}")
        return None
    if 'age' not in row:
        logger.warning(f"Skipping row without age: {row}")
        return None
    try:
        age = int(row['age'])
        if age < 0:
            logger.warning(f"Skipping row with negative age: {row}")
            return None
    except ValueError:
        logger.warning(f"Skipping row with non-integer age: {row}")
        return None
    # If we get here, the row is valid
    return row

def transform_row(row):
    """Transform a single row by converting age to integer and adding a new field.
    Returns the transformed row if successful, None otherwise.
    """
    try:
        row['age'] = int(row['age'])
        row['is_adult'] = row['age'] >= 18
        return row
    except ValueError:
        logger.warning(f"Skipping row due to invalid age conversion: {row}")
        return None

def load_rows(fieldnames, rows, file_path):
    """Load rows to a CSV file.
    Writes the header and then each row.
    """
    if fieldnames is None:
        logger.warning("No fieldnames to write.")
        return
    try:
        with open(file_path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            count = 0
            for row in rows:
                if row is not None:  # Skip None rows
                    writer.writerow(row)
                    count += 1
            logger.info(f"Successfully loaded {count} rows to {file_path}")
    except Exception as e:
        logger.error(f"An error occurred while writing the file: {e}")

if __name__ == "__main__":
    logger.info("Starting ETL process")
    extracted = extract('data.csv')
    try:
        fieldnames = next(extracted)  # Get the fieldnames
    except StopIteration:
        logger.error("No data extracted from 'data.csv'.")
        fieldnames = None
        extracted = []  # Empty iterator
    
    if fieldnames is not None:
        # Process rows: validate, then transform
        validated_rows = (validate_row(row) for row in extracted)
        transformed_rows = (transform_row(row) for row in validated_rows)
        # Load the transformed rows
        load_rows(fieldnames, transformed_rows, 'output.csv')
    else:
        logger.warning("Skipping load due to missing fieldnames.")
    
    logger.info("ETL process completed")