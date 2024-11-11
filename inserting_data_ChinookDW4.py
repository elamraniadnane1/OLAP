import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta

def connect_to_db(server, database):
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        'Trusted_Connection=yes;'
    )
    return conn

def truncate_string(value, max_length):
    if value is None:
        return None
    return value[:max_length]

def insert_artists(cursor, num_records):
    print(f"Inserting {num_records} artists...")
    faker = Faker()
    artist_ids = []
    for _ in range(num_records):
        name = truncate_string(faker.name(), 120)  # NVARCHAR(120)
        cursor.execute("SELECT MAX(ArtistId) FROM Artist")
        max_id_row = cursor.fetchone()
        artist_id = (max_id_row[0] or 0) + 1
        cursor.execute("INSERT INTO Artist (ArtistId, Name) VALUES (?, ?)", artist_id, name)
        artist_ids.append(artist_id)
    return artist_ids

def insert_albums(cursor, artist_ids, num_records):
    print(f"Inserting {num_records} albums...")
    faker = Faker()
    album_ids = []
    for _ in range(num_records):
        title = truncate_string(faker.sentence(nb_words=3), 160)  # NVARCHAR(160)
        artist_id = random.choice(artist_ids)
        cursor.execute("SELECT MAX(AlbumId) FROM Album")
        max_id_row = cursor.fetchone()
        album_id = (max_id_row[0] or 0) + 1
        cursor.execute("INSERT INTO Album (AlbumId, Title, ArtistId) VALUES (?, ?, ?)", album_id, title, artist_id)
        album_ids.append(album_id)
    return album_ids

def insert_genres(cursor, num_records):
    print(f"Inserting {num_records} genres...")
    faker = Faker()
    genre_ids = []
    for _ in range(num_records):
        name = truncate_string(faker.word().capitalize(), 120)  # NVARCHAR(120)
        cursor.execute("SELECT MAX(GenreId) FROM Genre")
        max_id_row = cursor.fetchone()
        genre_id = (max_id_row[0] or 0) + 1
        cursor.execute("INSERT INTO Genre (GenreId, Name) VALUES (?, ?)", genre_id, name)
        genre_ids.append(genre_id)
    return genre_ids

def insert_mediatypes(cursor, num_records):
    print(f"Inserting {num_records} media types...")
    faker = Faker()
    mediatype_ids = []
    for _ in range(num_records):
        name = truncate_string(faker.word().capitalize(), 120)  # NVARCHAR(120)
        cursor.execute("SELECT MAX(MediaTypeId) FROM MediaType")
        max_id_row = cursor.fetchone()
        mediatype_id = (max_id_row[0] or 0) + 1
        cursor.execute("INSERT INTO MediaType (MediaTypeId, Name) VALUES (?, ?)", mediatype_id, name)
        mediatype_ids.append(mediatype_id)
    return mediatype_ids

def insert_tracks(cursor, album_ids, genre_ids, mediatype_ids, num_records):
    print(f"Inserting {num_records} tracks...")
    faker = Faker()
    track_ids = []
    for _ in range(num_records):
        name = truncate_string(faker.sentence(nb_words=4), 200)  # NVARCHAR(200)
        album_id = random.choice(album_ids)
        mediatype_id = random.choice(mediatype_ids)
        genre_id = random.choice(genre_ids)
        composer = truncate_string(faker.name(), 220)  # NVARCHAR(220)
        milliseconds = random.randint(60000, 300000)
        bytes_size = milliseconds * random.randint(50, 150)
        unit_price = round(random.uniform(0.99, 1.99), 2)
        cursor.execute("SELECT MAX(TrackId) FROM Track")
        max_id_row = cursor.fetchone()
        track_id = (max_id_row[0] or 0) + 1
        cursor.execute("""
            INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, track_id, name, album_id, mediatype_id, genre_id, composer, milliseconds, bytes_size, unit_price)
        track_ids.append(track_id)
    return track_ids

def insert_employees(cursor, num_records):
    print(f"Inserting {num_records} employees...")
    faker = Faker()
    employee_ids = []
    for _ in range(num_records):
        first_name = truncate_string(faker.first_name(), 20)  # NVARCHAR(20)
        last_name = truncate_string(faker.last_name(), 20)    # NVARCHAR(20)
        title = truncate_string(random.choice(['Sales Manager', 'Sales Support Agent', 'IT Manager', 'IT Staff']), 30)  # NVARCHAR(30)
        reports_to = None  # For simplicity
        birth_date = faker.date_between(start_date='-60y', end_date='-25y')
        hire_date = faker.date_between(start_date='-20y', end_date='today')
        address = truncate_string(faker.street_address(), 70)  # NVARCHAR(70)
        city = truncate_string(faker.city(), 40)               # NVARCHAR(40)
        state = truncate_string(faker.state(), 40)             # NVARCHAR(40)
        country = truncate_string(faker.country(), 40)         # NVARCHAR(40)
        postal_code = truncate_string(faker.postcode(), 10)    # NVARCHAR(10)
        phone = truncate_string(faker.phone_number(), 24)      # NVARCHAR(24)
        fax = truncate_string(faker.phone_number(), 24)        # NVARCHAR(24)
        email = truncate_string(faker.email(), 60)             # NVARCHAR(60)
        cursor.execute("SELECT MAX(EmployeeId) FROM Employee")
        max_id_row = cursor.fetchone()
        employee_id = (max_id_row[0] or 0) + 1
        cursor.execute("""
            INSERT INTO Employee (EmployeeId, LastName, FirstName, Title, ReportsTo, BirthDate, HireDate,
                                  Address, City, State, Country, PostalCode, Phone, Fax, Email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, employee_id, last_name, first_name, title, reports_to, birth_date, hire_date,
             address, city, state, country, postal_code, phone, fax, email)
        employee_ids.append(employee_id)
    return employee_ids

def insert_customers(cursor, employee_ids, num_records):
    print(f"Inserting {num_records} customers...")
    faker = Faker()
    customer_ids = []
    for _ in range(num_records):
        first_name = truncate_string(faker.first_name(), 40)  # NVARCHAR(40)
        last_name = truncate_string(faker.last_name(), 20)    # NVARCHAR(20)
        company = truncate_string(faker.company(), 80)        # NVARCHAR(80)
        address = truncate_string(faker.street_address(), 70) # NVARCHAR(70)
        city = truncate_string(faker.city(), 40)              # NVARCHAR(40)
        state = truncate_string(faker.state(), 40)            # NVARCHAR(40)
        country = truncate_string(faker.country(), 40)        # NVARCHAR(40)
        postal_code = truncate_string(faker.postcode(), 10)   # NVARCHAR(10)
        phone = truncate_string(faker.phone_number(), 24)     # NVARCHAR(24)
        fax = truncate_string(faker.phone_number(), 24)       # NVARCHAR(24)
        email = truncate_string(faker.email(), 60)            # NVARCHAR(60)
        support_rep_id = random.choice(employee_ids) if employee_ids else None
        cursor.execute("SELECT MAX(CustomerId) FROM Customer")
        max_id_row = cursor.fetchone()
        customer_id = (max_id_row[0] or 0) + 1
        cursor.execute("""
            INSERT INTO Customer (CustomerId, FirstName, LastName, Company, Address, City, State,
                                  Country, PostalCode, Phone, Fax, Email, SupportRepId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, customer_id, first_name, last_name, company, address, city, state,
             country, postal_code, phone, fax, email, support_rep_id)
        customer_ids.append(customer_id)
    return customer_ids

def insert_invoices(cursor, customer_ids, num_records):
    print(f"Inserting {num_records} invoices...")
    faker = Faker()
    invoice_ids = []
    for _ in range(num_records):
        customer_id = random.choice(customer_ids)
        invoice_date = faker.date_between(start_date='-5y', end_date='today')
        billing_address = truncate_string(faker.street_address(), 70)  # NVARCHAR(70)
        billing_city = truncate_string(faker.city(), 40)               # NVARCHAR(40)
        billing_state = truncate_string(faker.state(), 40)             # NVARCHAR(40)
        billing_country = truncate_string(faker.country(), 40)         # NVARCHAR(40)
        billing_postal_code = truncate_string(faker.postcode(), 10)    # NVARCHAR(10)
        total = round(random.uniform(10.00, 500.00), 2)
        cursor.execute("SELECT MAX(InvoiceId) FROM Invoice")
        max_id_row = cursor.fetchone()
        invoice_id = (max_id_row[0] or 0) + 1
        cursor.execute("""
            INSERT INTO Invoice (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity,
                                 BillingState, BillingCountry, BillingPostalCode, Total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, invoice_id, customer_id, invoice_date, billing_address, billing_city,
             billing_state, billing_country, billing_postal_code, total)
        invoice_ids.append(invoice_id)
    return invoice_ids

def insert_invoicelines(cursor, invoice_ids, track_ids, num_records):
    print(f"Inserting {num_records} invoice lines...")
    invoice_line_ids = []
    for _ in range(num_records):
        invoice_id = random.choice(invoice_ids)
        track_id = random.choice(track_ids)
        unit_price = round(random.uniform(0.99, 1.99), 2)
        quantity = random.randint(1, 5)
        cursor.execute("SELECT MAX(InvoiceLineId) FROM InvoiceLine")
        max_id_row = cursor.fetchone()
        invoice_line_id = (max_id_row[0] or 0) + 1
        cursor.execute("""
            INSERT INTO InvoiceLine (InvoiceLineId, InvoiceId, TrackId, UnitPrice, Quantity)
            VALUES (?, ?, ?, ?, ?)
        """, invoice_line_id, invoice_id, track_id, unit_price, quantity)
        invoice_line_ids.append(invoice_line_id)
    return invoice_line_ids

def insert_playlists(cursor, num_records):
    print(f"Inserting {num_records} playlists...")
    faker = Faker()
    playlist_ids = []
    for _ in range(num_records):
        name = truncate_string(faker.sentence(nb_words=2), 120)  # NVARCHAR(120)
        cursor.execute("SELECT MAX(PlaylistId) FROM Playlist")
        max_id_row = cursor.fetchone()
        playlist_id = (max_id_row[0] or 0) + 1
        cursor.execute("INSERT INTO Playlist (PlaylistId, Name) VALUES (?, ?)", playlist_id, name)
        playlist_ids.append(playlist_id)
    return playlist_ids

def insert_playlisttracks(cursor, playlist_ids, track_ids, num_records):
    print(f"Inserting {num_records} playlist tracks...")
    for _ in range(num_records):
        playlist_id = random.choice(playlist_ids)
        track_id = random.choice(track_ids)
        cursor.execute("""
            SELECT COUNT(*) FROM PlaylistTrack WHERE PlaylistId = ? AND TrackId = ?
        """, playlist_id, track_id)
        exists = cursor.fetchone()[0]
        if not exists:
            cursor.execute("""
                INSERT INTO PlaylistTrack (PlaylistId, TrackId)
                VALUES (?, ?)
            """, playlist_id, track_id)

def main():
    # Database connection parameters
    source_server = 'DPC2023'  # Replace with your server name
    source_database = 'Chinook'
    
    # Connect to source database
    source_conn = connect_to_db(source_server, source_database)
    source_cursor = source_conn.cursor()
    
    # Start transaction
    source_conn.autocommit = False
    try:
        # Insert data
        artist_ids = insert_artists(source_cursor, num_records=10)
        genre_ids = insert_genres(source_cursor, num_records=5)
        mediatype_ids = insert_mediatypes(source_cursor, num_records=3)
        album_ids = insert_albums(source_cursor, artist_ids, num_records=20)
        track_ids = insert_tracks(source_cursor, album_ids, genre_ids, mediatype_ids, num_records=50)
        employee_ids = insert_employees(source_cursor, num_records=5)
        customer_ids = insert_customers(source_cursor, employee_ids, num_records=15)
        invoice_ids = insert_invoices(source_cursor, customer_ids, num_records=30)
        insert_invoicelines(source_cursor, invoice_ids, track_ids, num_records=100)
        playlist_ids = insert_playlists(source_cursor, num_records=5)
        insert_playlisttracks(source_cursor, playlist_ids, track_ids, num_records=50)
        
        # Commit transaction
        source_conn.commit()
        print("Data insertion completed successfully.")
    except Exception as e:
        # Rollback transaction on error
        source_conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        # Close connections
        source_cursor.close()
        source_conn.close()

if __name__ == "__main__":
    main()
