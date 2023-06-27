import csv

def read_csv_file(file_path):
    """
    Read data from a CSV file and process it.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        data (list): List of tuples representing the processed data from the CSV file.
    """
    
    data = []
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        # Skip the header row
        next(csv_reader)
        for row in csv_reader:
            processed_row = []
            for value in row:
                processed_row.append(value if value != '' else None)
            data.append(tuple(processed_row))
    return data



