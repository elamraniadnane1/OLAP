import pyodbc
import re
from datetime import datetime

def connect_to_db(server, database):
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        'Trusted_Connection=yes;'
    )
    return conn

def create_staging_tables(target_cursor, target_conn):
    print("Creating staging tables if they do not exist...")
    staging_tables_sql = [
        """
        IF OBJECT_ID('stg_Artist', 'U') IS NULL
        CREATE TABLE stg_Artist (
            ArtistId INT PRIMARY KEY,
            Name NVARCHAR(120)
        )
        """,
        """
        IF OBJECT_ID('stg_Album', 'U') IS NULL
        CREATE TABLE stg_Album (
            AlbumId INT PRIMARY KEY,
            Title NVARCHAR(160),
            ArtistId INT
        )
        """,
        """
        IF OBJECT_ID('stg_Genre', 'U') IS NULL
        CREATE TABLE stg_Genre (
            GenreId INT PRIMARY KEY,
            Name NVARCHAR(120)
        )
        """,
        """
        IF OBJECT_ID('stg_MediaType', 'U') IS NULL
        CREATE TABLE stg_MediaType (
            MediaTypeId INT PRIMARY KEY,
            Name NVARCHAR(120)
        )
        """,
        """
        IF OBJECT_ID('stg_Track', 'U') IS NULL
        CREATE TABLE stg_Track (
            TrackId INT PRIMARY KEY,
            Name NVARCHAR(200),
            AlbumId INT,
            MediaTypeId INT,
            GenreId INT,
            Composer NVARCHAR(220),
            Milliseconds INT,
            Bytes INT,
            UnitPrice NUMERIC(10,2)
        )
        """,
        """
        IF OBJECT_ID('stg_Employee', 'U') IS NULL
        CREATE TABLE stg_Employee (
            EmployeeId INT PRIMARY KEY,
            LastName NVARCHAR(20),
            FirstName NVARCHAR(20),
            Title NVARCHAR(30),
            ReportsTo INT,
            BirthDate DATETIME,
            HireDate DATETIME,
            Address NVARCHAR(70),
            City NVARCHAR(40),
            State NVARCHAR(40),
            Country NVARCHAR(40),
            PostalCode NVARCHAR(10),
            Phone NVARCHAR(24),
            Fax NVARCHAR(24),
            Email NVARCHAR(60)
        )
        """,
        """
        IF OBJECT_ID('stg_Customer', 'U') IS NULL
        CREATE TABLE stg_Customer (
            CustomerId INT PRIMARY KEY,
            FirstName NVARCHAR(40),
            LastName NVARCHAR(20),
            Company NVARCHAR(80),
            Address NVARCHAR(70),
            City NVARCHAR(40),
            State NVARCHAR(40),
            Country NVARCHAR(40),
            PostalCode NVARCHAR(10),
            Phone NVARCHAR(24),
            Fax NVARCHAR(24),
            Email NVARCHAR(60),
            SupportRepId INT
        )
        """,
        """
        IF OBJECT_ID('stg_Invoice', 'U') IS NULL
        CREATE TABLE stg_Invoice (
            InvoiceId INT PRIMARY KEY,
            CustomerId INT,
            InvoiceDate DATETIME,
            BillingAddress NVARCHAR(70),
            BillingCity NVARCHAR(40),
            BillingState NVARCHAR(40),
            BillingCountry NVARCHAR(40),
            BillingPostalCode NVARCHAR(10),
            Total NUMERIC(10,2)
        )
        """,
        """
        IF OBJECT_ID('stg_InvoiceLine', 'U') IS NULL
        CREATE TABLE stg_InvoiceLine (
            InvoiceLineId INT PRIMARY KEY,
            InvoiceId INT,
            TrackId INT,
            UnitPrice NUMERIC(10,2),
            Quantity INT
        )
        """
    ]
    for sql in staging_tables_sql:
        target_cursor.execute(sql)
    target_conn.commit()
    print("Staging tables created or verified.")

def truncate_staging_tables(target_cursor, target_conn):
    print("Truncating staging tables...")
    tables = ['stg_Artist', 'stg_Album', 'stg_Genre', 'stg_MediaType', 'stg_Track', 'stg_Employee', 'stg_Customer', 'stg_Invoice', 'stg_InvoiceLine']
    for table in tables:
        target_cursor.execute(f"TRUNCATE TABLE {table}")
    target_conn.commit()
    print("Staging tables truncated.")

def preprocess_artist(source_cursor, target_cursor):
    print("Preprocessing Artist data...")
    source_cursor.execute("SELECT ArtistId, Name FROM Artist")
    rows = source_cursor.fetchall()
    for row in rows:
        # Data Cleaning: Standardize names
        name = row.Name.strip().title() if row.Name else None
        target_cursor.execute("INSERT INTO stg_Artist (ArtistId, Name) VALUES (?, ?)", row.ArtistId, name)
    print("Artist data preprocessed and loaded into staging.")

def preprocess_album(source_cursor, target_cursor):
    print("Preprocessing Album data...")
    source_cursor.execute("SELECT AlbumId, Title, ArtistId FROM Album")
    rows = source_cursor.fetchall()
    for row in rows:
        title = row.Title.strip().title() if row.Title else None
        target_cursor.execute("INSERT INTO stg_Album (AlbumId, Title, ArtistId) VALUES (?, ?, ?)", row.AlbumId, title, row.ArtistId)
    print("Album data preprocessed and loaded into staging.")

def preprocess_genre(source_cursor, target_cursor):
    print("Preprocessing Genre data...")
    source_cursor.execute("SELECT GenreId, Name FROM Genre")
    rows = source_cursor.fetchall()
    for row in rows:
        name = row.Name.strip().title() if row.Name else None
        target_cursor.execute("INSERT INTO stg_Genre (GenreId, Name) VALUES (?, ?)", row.GenreId, name)
    print("Genre data preprocessed and loaded into staging.")

def preprocess_mediatype(source_cursor, target_cursor):
    print("Preprocessing MediaType data...")
    source_cursor.execute("SELECT MediaTypeId, Name FROM MediaType")
    rows = source_cursor.fetchall()
    for row in rows:
        name = row.Name.strip().title() if row.Name else None
        target_cursor.execute("INSERT INTO stg_MediaType (MediaTypeId, Name) VALUES (?, ?)", row.MediaTypeId, name)
    print("MediaType data preprocessed and loaded into staging.")

def preprocess_track(source_cursor, target_cursor):
    print("Preprocessing Track data...")
    source_cursor.execute("SELECT TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice FROM Track")
    rows = source_cursor.fetchall()
    for row in rows:
        name = row.Name.strip().title() if row.Name else None
        composer = row.Composer.strip().title() if row.Composer else None
        target_cursor.execute("""
            INSERT INTO stg_Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.TrackId, name, row.AlbumId, row.MediaTypeId, row.GenreId, composer, row.Milliseconds, row.Bytes, row.UnitPrice)
    print("Track data preprocessed and loaded into staging.")

def preprocess_employee(source_cursor, target_cursor):
    print("Preprocessing Employee data...")
    source_cursor.execute("""
        SELECT EmployeeId, LastName, FirstName, Title, ReportsTo, BirthDate, HireDate,
               Address, City, State, Country, PostalCode, Phone, Fax, Email
        FROM Employee
    """)
    rows = source_cursor.fetchall()
    for row in rows:
        # Data Cleaning: Standardize names and addresses
        last_name = row.LastName.strip().title() if row.LastName else None
        first_name = row.FirstName.strip().title() if row.FirstName else None
        title = row.Title.strip().title() if row.Title else None
        address = row.Address.strip().title() if row.Address else None
        city = row.City.strip().title() if row.City else None
        state = row.State.strip().title() if row.State else None
        country = row.Country.strip().title() if row.Country else None
        email = row.Email.strip().lower() if row.Email else None
        # Data Transformation: Validate email format
        email = email if re.match(r"[^@]+@[^@]+\.[^@]+", email) else None
        target_cursor.execute("""
            INSERT INTO stg_Employee (EmployeeId, LastName, FirstName, Title, ReportsTo, BirthDate, HireDate,
                                      Address, City, State, Country, PostalCode, Phone, Fax, Email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.EmployeeId, last_name, first_name, title, row.ReportsTo, row.BirthDate, row.HireDate,
             address, city, state, country, row.PostalCode, row.Phone, row.Fax, email)
    print("Employee data preprocessed and loaded into staging.")

def preprocess_customer(source_cursor, target_cursor):
    print("Preprocessing Customer data...")
    source_cursor.execute("""
        SELECT CustomerId, FirstName, LastName, Company, Address, City, State, Country, PostalCode,
               Phone, Fax, Email, SupportRepId
        FROM Customer
    """)
    rows = source_cursor.fetchall()
    for row in rows:
        first_name = row.FirstName.strip().title() if row.FirstName else None
        last_name = row.LastName.strip().title() if row.LastName else None
        company = row.Company.strip().title() if row.Company else None
        address = row.Address.strip().title() if row.Address else None
        city = row.City.strip().title() if row.City else None
        state = row.State.strip().title() if row.State else None
        country = row.Country.strip().title() if row.Country else None
        email = row.Email.strip().lower() if row.Email else None
        # Data Transformation: Validate email format
        email = email if re.match(r"[^@]+@[^@]+\.[^@]+", email) else None
        target_cursor.execute("""
            INSERT INTO stg_Customer (CustomerId, FirstName, LastName, Company, Address, City, State,
                                      Country, PostalCode, Phone, Fax, Email, SupportRepId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.CustomerId, first_name, last_name, company, address, city, state,
             country, row.PostalCode, row.Phone, row.Fax, email, row.SupportRepId)
    print("Customer data preprocessed and loaded into staging.")

def preprocess_invoice(source_cursor, target_cursor):
    print("Preprocessing Invoice data...")
    source_cursor.execute("""
        SELECT InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState,
               BillingCountry, BillingPostalCode, Total
        FROM Invoice
    """)
    rows = source_cursor.fetchall()
    for row in rows:
        billing_address = row.BillingAddress.strip().title() if row.BillingAddress else None
        billing_city = row.BillingCity.strip().title() if row.BillingCity else None
        billing_state = row.BillingState.strip().title() if row.BillingState else None
        billing_country = row.BillingCountry.strip().title() if row.BillingCountry else None
        target_cursor.execute("""
            INSERT INTO stg_Invoice (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity,
                                     BillingState, BillingCountry, BillingPostalCode, Total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.InvoiceId, row.CustomerId, row.InvoiceDate, billing_address, billing_city,
             billing_state, billing_country, row.BillingPostalCode, row.Total)
    print("Invoice data preprocessed and loaded into staging.")

def preprocess_invoiceline(source_cursor, target_cursor):
    print("Preprocessing InvoiceLine data...")
    source_cursor.execute("""
        SELECT InvoiceLineId, InvoiceId, TrackId, UnitPrice, Quantity
        FROM InvoiceLine
    """)
    rows = source_cursor.fetchall()
    for row in rows:
        target_cursor.execute("""
            INSERT INTO stg_InvoiceLine (InvoiceLineId, InvoiceId, TrackId, UnitPrice, Quantity)
            VALUES (?, ?, ?, ?, ?)
        """, row.InvoiceLineId, row.InvoiceId, row.TrackId, row.UnitPrice, row.Quantity)
    print("InvoiceLine data preprocessed and loaded into staging.")

def main():
    # Database connection parameters
    source_server = 'DPC2023'   # Replace with your source server name
    source_database = 'Chinook'
    target_server = 'DPC2023'   # Replace with your target server name
    target_database = 'ChinookDW4'

    # Connect to source and target databases
    source_conn = connect_to_db(source_server, source_database)
    target_conn = connect_to_db(target_server, target_database)

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    # Create staging tables if they do not exist
    create_staging_tables(target_cursor, target_conn)

    # Truncate staging tables
    truncate_staging_tables(target_cursor, target_conn)

    # Preprocess and load data into staging tables
    preprocess_artist(source_cursor, target_cursor)
    preprocess_album(source_cursor, target_cursor)
    preprocess_genre(source_cursor, target_cursor)
    preprocess_mediatype(source_cursor, target_cursor)
    preprocess_track(source_cursor, target_cursor)
    preprocess_employee(source_cursor, target_cursor)
    preprocess_customer(source_cursor, target_cursor)
    preprocess_invoice(source_cursor, target_cursor)
    preprocess_invoiceline(source_cursor, target_cursor)

    # Commit changes
    target_conn.commit()

    # Close connections
    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()
    print("Data preprocessing completed successfully.")

if __name__ == "__main__":
    main()
