import pyodbc
import pandas as pd

# Database connection parameters
source_server = 'DPC2023'  # Replace with your server name
source_database = 'Chinook'

# Establish the connection
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={source_server};'
    f'DATABASE={source_database};'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Fetch tables for cleaning
tables = ['Album', 'Artist', 'Customer', 'Employee', 'Genre', 'Invoice', 'InvoiceLine', 'MediaType', 'Playlist', 'PlaylistTrack', 'Track']

# Function to remove duplicate rows from a table
def remove_duplicates(table_name):
    primary_keys = {
        'Album': 'AlbumId',
        'Artist': 'ArtistId',
        'Customer': 'CustomerId',
        'Employee': 'EmployeeId',
        'Genre': 'GenreId',
        'Invoice': 'InvoiceId',
        'InvoiceLine': 'InvoiceLineId',
        'MediaType': 'MediaTypeId',
        'Playlist': 'PlaylistId',
        'PlaylistTrack': 'PlaylistId, TrackId',
        'Track': 'TrackId'
    }
    primary_key = primary_keys[table_name]
    if table_name == 'PlaylistTrack':
        cursor.execute(f"""
            WITH CTE AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY (SELECT NULL)) AS rn
                FROM {table_name}
            )
            DELETE FROM CTE WHERE rn > 1;
        """)
    else:
        cursor.execute(f"""
            WITH CTE AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY (SELECT NULL)) AS rn
                FROM {table_name}
            )
            DELETE FROM CTE WHERE rn > 1;
        """)
    conn.commit()
    print(f"Duplicates removed from {table_name}")

# Function to enforce NOT NULL constraints where necessary
def enforce_not_null_constraints():
    not_null_columns = {
        'Album': ['Title', 'ArtistId'],
        'Customer': ['FirstName', 'LastName', 'Email'],
        'Employee': ['LastName', 'FirstName'],
        'Track': ['Name', 'MediaTypeId', 'Milliseconds', 'UnitPrice']
    }
    for table, columns in not_null_columns.items():
        for column in columns:
            cursor.execute(f"""
                UPDATE {table}
                SET {column} = 'Unknown'
                WHERE {column} IS NULL;
            """)
            conn.commit()
            print(f"NULL values in {table}.{column} set to 'Unknown'")

# Function to remove inconsistencies based on Foreign Key relationships
def enforce_foreign_keys():
    foreign_keys = {
        'Album': {'ArtistId': 'Artist'},
        'Customer': {'SupportRepId': 'Employee'},
        'Invoice': {'CustomerId': 'Customer'},
        'InvoiceLine': {'InvoiceId': 'Invoice', 'TrackId': 'Track'},
        'PlaylistTrack': {'PlaylistId': 'Playlist', 'TrackId': 'Track'},
        'Track': {'AlbumId': 'Album', 'GenreId': 'Genre', 'MediaTypeId': 'MediaType'}
    }
    for table, fk_map in foreign_keys.items():
        for fk_column, ref_table in fk_map.items():
            cursor.execute(f"""
                DELETE FROM {table}
                WHERE {fk_column} NOT IN (SELECT {fk_column} FROM {ref_table});
            """)
            conn.commit()
            print(f"Inconsistent foreign keys removed from {table}.{fk_column}")

# Function to trim and clean up string columns
def trim_string_columns():
    string_columns = {
        'Customer': ['FirstName', 'LastName', 'Company', 'Address', 'City', 'State', 'Country', 'Phone', 'Fax', 'Email'],
        'Employee': ['LastName', 'FirstName', 'Title', 'Address', 'City', 'State', 'Country', 'Phone', 'Fax', 'Email'],
        'Track': ['Name', 'Composer']
    }
    for table, columns in string_columns.items():
        for column in columns:
            cursor.execute(f"""
                UPDATE {table}
                SET {column} = RTRIM(LTRIM({column}));
            """)
            conn.commit()
            print(f"Trimmed whitespace from {table}.{column}")

# Function to validate data types and ranges
def validate_data_types_and_ranges():
    validation_rules = {
        'Track': {'Milliseconds': 'Milliseconds > 0', 'UnitPrice': 'UnitPrice >= 0'},
        'Invoice': {'Total': 'Total >= 0'},
        'InvoiceLine': {'Quantity': 'Quantity > 0', 'UnitPrice': 'UnitPrice >= 0'}
    }
    for table, rules in validation_rules.items():
        for column, condition in rules.items():
            cursor.execute(f"""
                DELETE FROM {table}
                WHERE NOT ({condition});
            """)
            conn.commit()
            print(f"Invalid data removed from {table}.{column} based on condition: {condition}")

# Function to standardize country names
def standardize_country_names():
    country_standardization = {
        'USA': ['United States', 'US', 'USA'],
        'UK': ['United Kingdom', 'GB', 'UK']
    }
    for standard, variations in country_standardization.items():
        for variation in variations:
            cursor.execute(f"""
                UPDATE Customer
                SET Country = '{standard}'
                WHERE Country = '{variation}';
            """)
            conn.commit()
            print(f"Country name '{variation}' standardized to '{standard}'")

# Remove duplicates from each table
for table in tables:
    remove_duplicates(table)

# Enforce NOT NULL constraints
enforce_not_null_constraints()

# Enforce foreign key consistency
enforce_foreign_keys()

# Trim whitespace from string columns
trim_string_columns()

# Validate data types and ranges
validate_data_types_and_ranges()

# Standardize country names
standardize_country_names()

# Close the connection
cursor.close()
conn.close()
print("Data cleanup completed successfully.")
