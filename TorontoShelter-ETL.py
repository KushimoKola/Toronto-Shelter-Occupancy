# %%
import requests
import os
import csv
import re
from datetime import datetime
import hashlib

# Toronto Open Data is stored in a CKAN instance. Its APIs are documented here:
# https://docs.ckan.org/en/latest/api/

# Htting Toronto's open data  API
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
output_file_path = os.path.join(output_dir, "combined_results.csv")

# The logic here is to load existing idempotent keys from the previously processed CSV file (if it exists)
existing_idempotent_keys = set()
if os.path.exists(output_file_path):
    with open(output_file_path, "r", newline="", encoding="utf-8") as existing_file:
        existing_csv_reader = csv.reader(existing_file)
        next(existing_csv_reader)  # Skip header
        for row in existing_csv_reader:
            existing_idempotent_keys.add(row[0])  # Assuming idempotent_key is the first column

# This to determine count of total data and new data
total_data_count = 0
new_data_count = 0

with open(output_file_path, "a", newline="", encoding="utf-8") as output_file:
    csv_writer = csv.writer(output_file)
    current_header = None

    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):

        # for datastore_active resources:
        if resource["datastore_active"]:

            # To get all records in CSV format:
            url = base_url + "/datastore/dump/" + resource["id"]
            resource_dump_data = requests.get(url).text

            # Some columns were being dramatic, so, I had to clean each field by
            # removing commas and quotes within the fields
            cleaned_rows = []
            for row in resource_dump_data.split("\n"):
                cleaned_row = re.sub(r'"(.*?)"', lambda match: match.group(1).replace(",", ""), row)
                cleaned_rows.append(cleaned_row)

            # When data was appended, it appended headers also, causing a bit of problem
            #Since all column names are the same, I had to pick 1st index of the columns
            if idx == 0:
                current_header = cleaned_rows[0]
                csv_writer.writerow(["IDEMPOTENT_KEY"] + current_header.split(","))  # This added the indempotent_key to the header

            # Write the cleaned data to the CSV file, skip header for appended data
            for cleaned_row in cleaned_rows[1:]:
                if cleaned_row.strip():
                    data_fields = cleaned_row.split(",")

                    # This logic give a uniform OCCUPANCY_DATE to yyyy-mm-dd format
                    date_index = current_header.split(",").index("OCCUPANCY_DATE")
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

                    # Create idempotent key by combining _id and OCCUPANCY_DATE
                    id_index = current_header.split(",").index("_id")
                    id_value = data_fields[id_index]
                    idempotent_key = f"{id_value}_{formatted_date}"

                    # Hash the idempotent key for uniqueness
                    hashed_key = hashlib.sha3_224(idempotent_key.encode()).hexdigest()[:20]
                    
                    if hashed_key not in existing_idempotent_keys:
                        csv_writer.writerow([hashed_key] + data_fields)
                        new_data_count += 1
                        existing_idempotent_keys.add(hashed_key)

                    total_data_count += 1

print("Data processing complete.")
print("Total data count:", total_data_count)
print("New data count:", new_data_count)