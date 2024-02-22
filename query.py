import psycopg2
import csv
import os

class Database:
    # We connect to the server
    def __init__(self):
        self.postgresql_connection = psycopg2.connect(
            host='postgres_container',
            user='postgres',
            password='postgres',
            dbname='postgres'
        )

    # We select every entire table
    def retrieve_data_from_postgresql(self):
        cursor = self.postgresql_connection.cursor()
        cursor.execute("SELECT * FROM raw_table")
        data = cursor.fetchall()
        cursor.close()
        return data

    # We write it to csv file
    def write_to_csv(self, data, filename='output.csv'):
        output_dir = '/output'
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write header
            csvwriter.writerow([
                "slug", "language", "languages", "req_id", "title", "description",
                "street_address", "city", "state", "country_code", "postal_code",
                "location_type", "latitude", "longitude", "categories", "tags", "tags5",
                "tags6", "brand", "promotion_value", "salary_currency", "salary_value",
                "salary_min_value", "salary_max_value", "benefits", "employment_type",
                "hiring_organization", "source", "apply_url", "internal", "searchable",
                "applyable", "li_easy_applyable", "ats_code", "update_date",
                "create_date", "category", "full_location", "short_location"
            ])
            # Write data rows
            for row in data:
                csvwriter.writerow(row)

    # We close connection
    def close_connections(self):
        self.postgresql_connection.close()

if __name__ == "__main__":
    db = Database()
    data = db.retrieve_data_from_postgresql()
    db.write_to_csv(data)
    db.close_connections()