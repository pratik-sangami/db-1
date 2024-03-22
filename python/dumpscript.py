import sys
import csv
import traceback
import time
import psycopg2
from pymongo import MongoClient
from collections import defaultdict
import argparse

def print_usage():
    """Print usage instructions."""
    print("Usage: python3 dumpscript.py -f filename.ext -t [mongo|postgres]")
    print("Options:")
    print("  -f, --file     Specify the CSV file to read")
    print("  -t, --type     Specify the type of database to use (mongo or postgres)")
    print("  --help         Display this help message")

def process_csv(filename):
    """Process CSV file and determine column data types."""
    column_data_types = defaultdict(set)
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            columns = next(reader)
            for row in reader:
                for i, value in enumerate(row):
                    try:
                        int(value)
                        column_data_types[columns[i]].add('int')
                    except ValueError:
                        try:
                            float(value)
                            if 'int' not in column_data_types[columns[i]]:
                                column_data_types[columns[i]].add('float')
                        except ValueError:
                            column_data_types[columns[i]].add('varchar')
    except Exception as e:
        print(f"Error processing CSV: {e}")
        traceback.print_exc()
    return columns, column_data_types

def create_postgres_table(cur, table_name, column_data_types):
    """Create PostgreSQL table."""
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column, data_types in column_data_types.items():
        if 'varchar' in data_types:
            data_types.discard('float')
            data_types.discard('int')
        elif 'float' in data_types:
            data_types.discard('int')
        elif len(data_types) > 1 and 'int' in data_types:
            data_types.discard('int')
        data_type = ' '.join(data_types)
        create_table_query += f"{column} {data_type}, "
    create_table_query = create_table_query[:-2] + ")"
    cur.execute(create_table_query)

def insert_into_postgres(filename, table_name, columns, column_data_types):
    """Insert data into PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="lfkoP@ssw0rd",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        create_postgres_table(cur, table_name, column_data_types)
        conn.commit()

        start_time = time.time()
        total_rows = 0
        rows_added = 0
        errors = 0
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                total_rows += 1
                try:
                    row = [int(float(cell)) if '.' in cell and cell.replace('.', '', 1).isdigit() else float(cell) if cell.replace('.', '', 1).isdigit() else cell for cell in row]
                    cur.execute(
                        f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(row))})",
                        row
                    )
                    rows_added += 1
                except ValueError as e:
                    errors += 1
                    print(f"Error inserting row {total_rows}: {e}")
                    continue

        conn.commit()
        cur.close()
        conn.close()

        elapsed_time = time.time() - start_time
        print("Table created successfully in PostgreSQL!")
        print("Data inserted successfully into PostgreSQL!")
        print(f"Time elapsed: {elapsed_time:.2f} seconds")
        print(f"Total rows processed: {total_rows}")
        print(f"Total rows added: {rows_added}")
        print(f"Total errors encountered: {errors}")
    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")
        traceback.print_exc()

def insert_into_mongo(filename, table_name):
    """Insert data into MongoDB."""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['your_database_name']
        collection = db[table_name]

        start_time = time.time()
        total_rows = 0
        rows_added = 0
        errors = 0
        inserted_ids = []
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                total_rows += 1
                try:
                    result = collection.insert_one(row)
                    inserted_ids.append(result.inserted_id)
                    rows_added += 1
                except Exception as e:
                    errors += 1
                    print(f"Error inserting row {total_rows}: {e}")

        elapsed_time = time.time() - start_time
        print("Data inserted successfully into MongoDB!")
        print(f"Time elapsed: {elapsed_time:.2f} seconds")
        print(f"Total rows processed: {total_rows}")
        print(f"Total rows added: {rows_added}")
        print(f"Total errors encountered: {errors}")
    except Exception as e:
        print(f"Error inserting into MongoDB: {e}")
        traceback.print_exc()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Dump CSV data into a database.")
    parser.add_argument('-f', '--file', type=str, help="CSV file to read", required=True)
    parser.add_argument('-t', '--type', type=str, help="Type of database to use (mongo or postgres)", required=True)
    args = parser.parse_args()

    filename = args.file
    db_type = args.type

    columns, column_data_types = process_csv(filename)
    table_name = filename.split('/')[-1].split('.')[0]

    if db_type == 'postgres':
        insert_into_postgres(filename, table_name, columns, column_data_types)
    elif db_type == 'mongo':
        insert_into_mongo(filename, table_name)
    else:
        print("Invalid database type. Supported types are 'mongo' and 'postgres'.")

if __name__ == "__main__":
    main()
