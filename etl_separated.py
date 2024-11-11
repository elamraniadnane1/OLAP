import pyodbc
from datetime import datetime


import logging

logging.basicConfig(filename='etl_log.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

logging.info('ETL process started.')

def truncate_tables(target_cursor, target_conn):
    print("Deleting data from Dimension and Fact Tables...")
    tables = ['FactSales', 'DimCustomer', 'DimEmployee', 'DimDate', 'DimTrack', 'DimMediaType', 'DimGenre', 'DimAlbum', 'DimArtist']
    for table in tables:
        target_cursor.execute(f"DELETE FROM {table}")
        print(f"Data deleted from table {table}.")
    target_conn.commit()

def load_dim_artist(source_cursor, target_cursor, target_conn):
    print("Loading DimArtist...")
    # Fetch existing ArtistIds
    target_cursor.execute("SELECT ArtistId FROM DimArtist")
    existing_artist_ids = set(row.ArtistId for row in target_cursor.fetchall())

    source_cursor.execute("SELECT ArtistId, Name FROM Artist")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.ArtistId not in existing_artist_ids]

    for row in new_rows:
        target_cursor.execute("INSERT INTO DimArtist (ArtistId, Name) VALUES (?, ?)", row.ArtistId, row.Name)
    target_conn.commit()
    print(f"DimArtist loaded. {len(new_rows)} new records inserted.")

def load_dim_album(source_cursor, target_cursor, target_conn):
    print("Loading DimAlbum...")
    # Fetch existing AlbumIds
    target_cursor.execute("SELECT AlbumId FROM DimAlbum")
    existing_album_ids = set(row.AlbumId for row in target_cursor.fetchall())

    source_cursor.execute("SELECT AlbumId, Title, ArtistId FROM Album")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.AlbumId not in existing_album_ids]

    for row in new_rows:
        target_cursor.execute("SELECT ArtistKey FROM DimArtist WHERE ArtistId = ?", row.ArtistId)
        artist_key_row = target_cursor.fetchone()
        artist_key = artist_key_row.ArtistKey if artist_key_row else None
        target_cursor.execute("INSERT INTO DimAlbum (AlbumId, Title, ArtistKey) VALUES (?, ?, ?)", row.AlbumId, row.Title, artist_key)
    target_conn.commit()
    print(f"DimAlbum loaded. {len(new_rows)} new records inserted.")

def load_dim_genre(source_cursor, target_cursor, target_conn):
    print("Loading DimGenre...")
    # Fetch existing GenreIds
    target_cursor.execute("SELECT GenreId FROM DimGenre")
    existing_genre_ids = set(row.GenreId for row in target_cursor.fetchall())

    source_cursor.execute("SELECT GenreId, Name FROM Genre")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.GenreId not in existing_genre_ids]

    for row in new_rows:
        target_cursor.execute("INSERT INTO DimGenre (GenreId, Name) VALUES (?, ?)", row.GenreId, row.Name)
    target_conn.commit()
    print(f"DimGenre loaded. {len(new_rows)} new records inserted.")

def load_dim_mediatype(source_cursor, target_cursor, target_conn):
    print("Loading DimMediaType...")
    # Fetch existing MediaTypeIds
    target_cursor.execute("SELECT MediaTypeId FROM DimMediaType")
    existing_mediatype_ids = set(row.MediaTypeId for row in target_cursor.fetchall())

    source_cursor.execute("SELECT MediaTypeId, Name FROM MediaType")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.MediaTypeId not in existing_mediatype_ids]

    for row in new_rows:
        target_cursor.execute("INSERT INTO DimMediaType (MediaTypeId, Name) VALUES (?, ?)", row.MediaTypeId, row.Name)
    target_conn.commit()
    print(f"DimMediaType loaded. {len(new_rows)} new records inserted.")

def load_dim_track(source_cursor, target_cursor, target_conn):
    print("Loading DimTrack...")
    # Fetch existing TrackIds
    target_cursor.execute("SELECT TrackId FROM DimTrack")
    existing_track_ids = set(row.TrackId for row in target_cursor.fetchall())

    source_cursor.execute("SELECT TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes FROM Track")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.TrackId not in existing_track_ids]

    for row in new_rows:
        # Get AlbumKey
        target_cursor.execute("SELECT AlbumKey FROM DimAlbum WHERE AlbumId = ?", row.AlbumId)
        album_key_row = target_cursor.fetchone()
        album_key = album_key_row.AlbumKey if album_key_row else None

        # Get MediaTypeKey
        target_cursor.execute("SELECT MediaTypeKey FROM DimMediaType WHERE MediaTypeId = ?", row.MediaTypeId)
        mediatype_key_row = target_cursor.fetchone()
        mediatype_key = mediatype_key_row.MediaTypeKey if mediatype_key_row else None

        # Get GenreKey
        target_cursor.execute("SELECT GenreKey FROM DimGenre WHERE GenreId = ?", row.GenreId)
        genre_key_row = target_cursor.fetchone()
        genre_key = genre_key_row.GenreKey if genre_key_row else None

        target_cursor.execute("""
            INSERT INTO DimTrack (TrackId, Name, AlbumKey, MediaTypeKey, GenreKey, Composer, Milliseconds, Bytes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row.TrackId, row.Name, album_key, mediatype_key, genre_key, row.Composer, row.Milliseconds, row.Bytes)
    target_conn.commit()
    print(f"DimTrack loaded. {len(new_rows)} new records inserted.")

def load_dim_employee(source_cursor, target_cursor, target_conn):
    print("Loading DimEmployee...")
    # Fetch existing EmployeeIds
    target_cursor.execute("SELECT EmployeeId FROM DimEmployee")
    existing_employee_ids = set(row.EmployeeId for row in target_cursor.fetchall())

    source_cursor.execute("SELECT EmployeeId, FirstName, LastName, Title, ReportsTo, HireDate FROM Employee")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.EmployeeId not in existing_employee_ids]

    for row in new_rows:
        target_cursor.execute("""
            INSERT INTO DimEmployee (EmployeeId, FirstName, LastName, Title, ReportsTo, HireDate)
            VALUES (?, ?, ?, ?, ?, ?)
        """, row.EmployeeId, row.FirstName, row.LastName, row.Title, row.ReportsTo, row.HireDate)
    target_conn.commit()
    print(f"DimEmployee loaded. {len(new_rows)} new records inserted.")

def load_dim_customer(source_cursor, target_cursor, target_conn):
    print("Loading DimCustomer...")
    # Fetch existing CustomerIds
    target_cursor.execute("SELECT CustomerId FROM DimCustomer")
    existing_customer_ids = set(row.CustomerId for row in target_cursor.fetchall())

    source_cursor.execute("""
        SELECT CustomerId, FirstName, LastName, Company, Address, City, State, Country, PostalCode
        FROM Customer
    """)
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.CustomerId not in existing_customer_ids]

    for row in new_rows:
        target_cursor.execute("""
            INSERT INTO DimCustomer (CustomerId, FirstName, LastName, Company, Address, City, State, Country, PostalCode)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.CustomerId, row.FirstName, row.LastName, row.Company, row.Address, row.City, row.State, row.Country, row.PostalCode)
    target_conn.commit()
    print(f"DimCustomer loaded. {len(new_rows)} new records inserted.")

def load_dim_date(source_cursor, target_cursor, target_conn):
    print("Loading DimDate...")
    # Fetch existing Dates
    target_cursor.execute("SELECT Date FROM DimDate")
    existing_dates = set(row.Date for row in target_cursor.fetchall())

    source_cursor.execute("SELECT DISTINCT CAST(InvoiceDate AS DATE) AS Date FROM Invoice")
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.Date not in existing_dates]

    for row in new_rows:
        date_value = row.Date
        day = date_value.day
        month = date_value.month
        year = date_value.year
        quarter = (month - 1) // 3 + 1
        target_cursor.execute("""
            INSERT INTO DimDate (Date, Day, Month, Year, Quarter)
            VALUES (?, ?, ?, ?, ?)
        """, date_value, day, month, year, quarter)
    target_conn.commit()
    print(f"DimDate loaded. {len(new_rows)} new records inserted.")

def build_mappings(target_cursor):
    print("Building mappings from natural keys to surrogate keys...")
    mappings = {}

    # ArtistId to ArtistKey
    target_cursor.execute("SELECT ArtistId, ArtistKey FROM DimArtist")
    mappings['Artist'] = {row.ArtistId: row.ArtistKey for row in target_cursor.fetchall()}

    # AlbumId to AlbumKey
    target_cursor.execute("SELECT AlbumId, AlbumKey FROM DimAlbum")
    mappings['Album'] = {row.AlbumId: row.AlbumKey for row in target_cursor.fetchall()}

    # GenreId to GenreKey
    target_cursor.execute("SELECT GenreId, GenreKey FROM DimGenre")
    mappings['Genre'] = {row.GenreId: row.GenreKey for row in target_cursor.fetchall()}

    # MediaTypeId to MediaTypeKey
    target_cursor.execute("SELECT MediaTypeId, MediaTypeKey FROM DimMediaType")
    mappings['MediaType'] = {row.MediaTypeId: row.MediaTypeKey for row in target_cursor.fetchall()}

    # TrackId to TrackKey
    target_cursor.execute("SELECT TrackId, TrackKey FROM DimTrack")
    mappings['Track'] = {row.TrackId: row.TrackKey for row in target_cursor.fetchall()}

    # Date to DateKey
    target_cursor.execute("SELECT Date, DateKey FROM DimDate")
    mappings['Date'] = {row.Date: row.DateKey for row in target_cursor.fetchall()}

    # CustomerId to CustomerKey
    target_cursor.execute("SELECT CustomerId, CustomerKey FROM DimCustomer")
    mappings['Customer'] = {row.CustomerId: row.CustomerKey for row in target_cursor.fetchall()}

    # EmployeeId to EmployeeKey
    target_cursor.execute("SELECT EmployeeId, EmployeeKey FROM DimEmployee")
    mappings['Employee'] = {row.EmployeeId: row.EmployeeKey for row in target_cursor.fetchall()}

    print("Mappings built.")
    return mappings

def load_fact_sales(source_cursor, target_cursor, target_conn, mappings):
    print("Loading FactSales...")
    # Fetch existing InvoiceLineIds
    target_cursor.execute("SELECT InvoiceLineId FROM FactSales")
    existing_invoice_line_ids = set(row.InvoiceLineId for row in target_cursor.fetchall())

    source_cursor.execute("""
    SELECT il.InvoiceLineId, il.InvoiceId, il.TrackId, il.Quantity, il.UnitPrice,
           i.InvoiceDate, i.CustomerId,
           t.AlbumId, t.GenreId, t.MediaTypeId,
           c.SupportRepId
    FROM InvoiceLine il
    JOIN Invoice i ON il.InvoiceId = i.InvoiceId
    JOIN Track t ON il.TrackId = t.TrackId
    JOIN Customer c ON i.CustomerId = c.CustomerId
    """)
    rows = source_cursor.fetchall()
    new_rows = [row for row in rows if row.InvoiceLineId not in existing_invoice_line_ids]

    for row in new_rows:
        InvoiceLineId = row.InvoiceLineId
        InvoiceDate = row.InvoiceDate.date()
        DateKey = mappings['Date'].get(InvoiceDate, None)
        CustomerKey = mappings['Customer'].get(row.CustomerId, None)
        TrackKey = mappings['Track'].get(row.TrackId, None)
        AlbumKey = mappings['Album'].get(row.AlbumId, None)
        GenreKey = mappings['Genre'].get(row.GenreId, None)
        MediaTypeKey = mappings['MediaType'].get(row.MediaTypeId, None)
        EmployeeKey = mappings['Employee'].get(row.SupportRepId, None)
        Quantity = row.Quantity
        UnitPrice = row.UnitPrice
        TotalAmount = Quantity * UnitPrice

        target_cursor.execute("""
            INSERT INTO FactSales (InvoiceLineId, DateKey, CustomerKey, TrackKey, AlbumKey, GenreKey, MediaTypeKey, EmployeeKey, Quantity, UnitPrice, TotalAmount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, InvoiceLineId, DateKey, CustomerKey, TrackKey, AlbumKey, GenreKey, MediaTypeKey, EmployeeKey, Quantity, UnitPrice, TotalAmount)
    target_conn.commit()
    print(f"FactSales loaded. {len(new_rows)} new records inserted.")

def main():
    # Database connection parameters
    source_server = 'DPC2023'
    source_database = 'Chinook'
    target_server = 'DPC2023'
    target_database = 'ChinookDW4'

    # Connect to source and target databases
    source_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+source_server+';DATABASE='+source_database+';Trusted_Connection=yes;')
    target_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+target_server+';DATABASE='+target_database+';Trusted_Connection=yes;')

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    # Prompt user to reset DW
    reset_dw = input("Do you want to reset the Data Warehouse? (yes/no): ").lower()
    if reset_dw == 'yes':
        truncate_tables(target_cursor, target_conn)

    # Load dimension tables
    load_dim_artist(source_cursor, target_cursor, target_conn)
    load_dim_album(source_cursor, target_cursor, target_conn)
    load_dim_genre(source_cursor, target_cursor, target_conn)
    load_dim_mediatype(source_cursor, target_cursor, target_conn)
    load_dim_track(source_cursor, target_cursor, target_conn)
    load_dim_employee(source_cursor, target_cursor, target_conn)
    load_dim_customer(source_cursor, target_cursor, target_conn)
    load_dim_date(source_cursor, target_cursor, target_conn)

    # Build mappings
    mappings = build_mappings(target_cursor)

    # Load FactSales
    load_fact_sales(source_cursor, target_cursor, target_conn, mappings)

    # Close connections
    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()
    print("ETL process completed.")

if __name__ == "__main__":
    main()
    logging.info('ETL process completed successfully.')
