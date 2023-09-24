# %%
import requests
import os
import csv
import re
from datetime import datetime
import hashlib

# Toronto Open Data is stored in a CKAN instance. Its APIs are documented here:
# https://docs.ckan.org/en/latest/api/

# Hitting Toronto's open data API
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# Datasets are called "packages". Each package can contain many "resources"
# To retrieve the metadata for this package and its resources, use the package name in this page's URL:
url = base_url + "/api/3/action/package_show"
params = {"id": "daily-shelter-overnight-service-occupancy-capacity"}
package = requests.get(url, params=params).json()

# I created a directory to store the CSV files
output_dir = os.getcwd()  # This is to get my current working directory
os.makedirs(output_dir, exist_ok=True)

# Since Data were in different files, I had to create a single directory to store all the files
output_file_path = os.path.join(output_dir, "toronto_shelter_occupancy.csv")

# This to determine count of total data and new data
total_data_count = 0
new_data_count = 0

# Logic to determine if header has been appended
header_appended = False
current_header = None

# Function to clean data
def clean_data(row):
    cleaned_row = re.sub(r'"(.*?)"', lambda match: match.group(1).replace(",", ""), row)
    return cleaned_row

# Create a set to store existing idempotent keys
existing_idempotent_keys = set()

# Check if the output file already exists
if os.path.exists(output_file_path):
    # Read existing idempotent keys from the previously processed CSV file
    with open(output_file_path, "r", newline="", encoding="utf-8") as existing_file:
        existing_csv_reader = csv.reader(existing_file)
        first_row = next(existing_csv_reader, None)  # Attempt to read the first row (header)
        if first_row:
            header_appended = True
            current_header = first_row[1:]  # Skip the first column (IDEMPOTENT_KEY)
            for row in existing_csv_reader:
                existing_idempotent_keys.add(row[0])  # Assuming idempotent_key is the first column

# Open the output file for writing, with proper newline handling
with open(output_file_path, "a", newline="", encoding="utf-8") as output_file:
    csv_writer = csv.writer(output_file)

    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):

        # for datastore_active resources:
        if resource["datastore_active"]:
            # To get all records in CSV format:
            url = base_url + "/datastore/dump/" + resource["id"]
            resource_dump_data = requests.get(url).text

            # Split data into rows
            rows = resource_dump_data.split("\n")

            # Process each row
            for row in rows:
                if row.strip():
                    # This is the first run or header has not been appended
                    if not header_appended:
                        current_header = clean_data(row).split(",")
                        csv_writer.writerow(["IDEMPOTENT_KEY"] + current_header)  # This added the idempotent_key to the header
                        header_appended = True
                    else:
                        # This is not the first run, process and append new data
                        data_fields = clean_data(row).split(",")
                        
                        # This logic gives a uniform OCCUPANCY_DATE to yyyy-mm-dd format
                        date_index = current_header.index("OCCUPANCY_DATE")
                        original_date = data_fields[date_index]

                        # Assumptions of different date formats - I handle the different date formats here
                        formatted_date = None
                        try:
                            formatted_date = datetime.strptime(original_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                        except ValueError:
                            try:
                                formatted_date = datetime.strptime(original_date, "%y-%m-%d").strftime("%Y-%m-%d")
                            except ValueError:
                                pass  # If I have missed any other date formats, so it doesn't throw an error

                        if formatted_date:
                            data_fields[date_index] = formatted_date

                        # Create an idempotent key by combining _id and OCCUPANCY_DATE
                        id_index = current_header.index("_id")
                        id_value = data_fields[id_index]
                        
                        # Exclude header rows from idempotent key creation
                        if id_value != "_id":
                            idempotent_key = f"{id_value}_{formatted_date}"

                            # Hash the idempotent key for uniqueness
                            hashed_key = hashlib.sha3_224(idempotent_key.encode()).hexdigest()[:20]

                            # Check if the hashed_key already exists
                            if hashed_key not in existing_idempotent_keys:
                                csv_writer.writerow([hashed_key] + data_fields)
                                new_data_count += 1

                    total_data_count += 1

print("Data processing complete.")
print("Total data count:", total_data_count)
print("New data count:", new_data_count)

