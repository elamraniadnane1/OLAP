/*******************************************************************************
   Drop and Create ChinookDW4 Database
********************************************************************************/
IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'ChinookDW4')
BEGIN
    ALTER DATABASE [ChinookDW4] SET OFFLINE WITH ROLLBACK IMMEDIATE;
    ALTER DATABASE [ChinookDW4] SET ONLINE;
    DROP DATABASE [ChinookDW4];
END
GO

CREATE DATABASE [ChinookDW4];
GO

USE [ChinookDW4];
GO

/*******************************************************************************
   Create Dimension and Fact Tables in ChinookDW4
********************************************************************************/

-- DimArtist
CREATE TABLE DimArtist (
    ArtistKey INT IDENTITY(1,1) PRIMARY KEY,
    ArtistId INT,
    Name NVARCHAR(120)
);

-- DimAlbum
CREATE TABLE DimAlbum (
    AlbumKey INT IDENTITY(1,1) PRIMARY KEY,
    AlbumId INT,
    Title NVARCHAR(160),
    ArtistKey INT,
    FOREIGN KEY (ArtistKey) REFERENCES DimArtist(ArtistKey)
);

-- DimTrack
CREATE TABLE DimTrack (
    TrackKey INT IDENTITY(1,1) PRIMARY KEY,
    TrackId INT,
    Name NVARCHAR(200),
    AlbumKey INT,
    MediaTypeKey INT,
    GenreKey INT,
    Composer NVARCHAR(220),
    Milliseconds INT,
    Bytes INT
);

-- DimGenre
CREATE TABLE DimGenre (
    GenreKey INT IDENTITY(1,1) PRIMARY KEY,
    GenreId INT,
    Name NVARCHAR(120)
);

-- DimMediaType
CREATE TABLE DimMediaType (
    MediaTypeKey INT IDENTITY(1,1) PRIMARY KEY,
    MediaTypeId INT,
    Name NVARCHAR(120)
);

-- DimDate
CREATE TABLE DimDate (
    DateKey INT IDENTITY(1,1) PRIMARY KEY,
    Date DATE,
    Day INT,
    Month INT,
    Year INT,
    Quarter INT
);

-- DimEmployee
CREATE TABLE DimEmployee (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeId INT,
    FirstName NVARCHAR(20),
    LastName NVARCHAR(20),
    Title NVARCHAR(30),
    ReportsTo INT,
    HireDate DATETIME
);

-- DimCustomer
CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId INT,
    FirstName NVARCHAR(40),
    LastName NVARCHAR(20),
    Company NVARCHAR(80),
    Address NVARCHAR(70),
    City NVARCHAR(40),
    State NVARCHAR(40),
    Country NVARCHAR(40),
    PostalCode NVARCHAR(10)
);

-- FactSales
CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceLineId INT,
    DateKey INT,
    CustomerKey INT,
    TrackKey INT,
    AlbumKey INT,
    GenreKey INT,
    MediaTypeKey INT,
    EmployeeKey INT,
    Quantity INT,
    UnitPrice NUMERIC(10,2),
    TotalAmount NUMERIC(10,2),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    FOREIGN KEY (TrackKey) REFERENCES DimTrack(TrackKey),
    FOREIGN KEY (AlbumKey) REFERENCES DimAlbum(AlbumKey),
    FOREIGN KEY (GenreKey) REFERENCES DimGenre(GenreKey),
    FOREIGN KEY (MediaTypeKey) REFERENCES DimMediaType(MediaTypeKey),
    FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey)
);

GO

/*******************************************************************************
   Populate Dimension Tables from ChinookDB
********************************************************************************/

USE Chinook;

-- DimArtist
INSERT INTO ChinookDW4.dbo.DimArtist (ArtistId, Name)
SELECT ArtistId, Name
FROM Artist;

-- DimAlbum
INSERT INTO ChinookDW4.dbo.DimAlbum (AlbumId, Title, ArtistKey)
SELECT AlbumId, Title, (SELECT ArtistKey FROM ChinookDW4.dbo.DimArtist WHERE ArtistId = Album.ArtistId)
FROM Album;

-- DimGenre
INSERT INTO ChinookDW4.dbo.DimGenre (GenreId, Name)
SELECT GenreId, Name
FROM Genre;

-- DimMediaType
INSERT INTO ChinookDW4.dbo.DimMediaType (MediaTypeId, Name)
SELECT MediaTypeId, Name
FROM MediaType;

-- DimTrack
INSERT INTO ChinookDW4.dbo.DimTrack (TrackId, Name, AlbumKey, MediaTypeKey, GenreKey, Composer, Milliseconds, Bytes)
SELECT TrackId, Name, 
       (SELECT AlbumKey FROM ChinookDW4.dbo.DimAlbum WHERE AlbumId = Track.AlbumId),
       (SELECT MediaTypeKey FROM ChinookDW4.dbo.DimMediaType WHERE MediaTypeId = Track.MediaTypeId),
       (SELECT GenreKey FROM ChinookDW4.dbo.DimGenre WHERE GenreId = Track.GenreId),
       Composer, Milliseconds, Bytes
FROM Track;

-- DimEmployee
INSERT INTO ChinookDW4.dbo.DimEmployee (EmployeeId, FirstName, LastName, Title, ReportsTo, HireDate)
SELECT EmployeeId, FirstName, LastName, Title, ReportsTo, HireDate
FROM Employee;

-- DimCustomer
INSERT INTO ChinookDW4.dbo.DimCustomer (CustomerId, FirstName, LastName, Company, Address, City, State, Country, PostalCode)
SELECT CustomerId, FirstName, LastName, Company, Address, City, State, Country, PostalCode
FROM Customer;

-- DimDate (Unique dates from InvoiceDate)
INSERT INTO ChinookDW4.dbo.DimDate (Date, Day, Month, Year, Quarter)
SELECT DISTINCT CAST(InvoiceDate AS DATE) AS Date,
       DATEPART(DAY, InvoiceDate) AS Day,
       DATEPART(MONTH, InvoiceDate) AS Month,
       DATEPART(YEAR, InvoiceDate) AS Year,
       DATEPART(QUARTER, InvoiceDate) AS Quarter
FROM Invoice;

GO

/*******************************************************************************
   Populate FactSales Table from ChinookDB
********************************************************************************/

INSERT INTO ChinookDW4.dbo.FactSales (InvoiceLineId, DateKey, CustomerKey, TrackKey, AlbumKey, GenreKey, MediaTypeKey, EmployeeKey, Quantity, UnitPrice, TotalAmount)
SELECT InvoiceLine.InvoiceLineId,
       (SELECT DateKey FROM ChinookDW4.dbo.DimDate WHERE Date = CAST(Invoice.InvoiceDate AS DATE)),
       (SELECT CustomerKey FROM ChinookDW4.dbo.DimCustomer WHERE CustomerId = Invoice.CustomerId),
       (SELECT TrackKey FROM ChinookDW4.dbo.DimTrack WHERE TrackId = InvoiceLine.TrackId),
       (SELECT AlbumKey FROM ChinookDW4.dbo.DimAlbum WHERE AlbumId = Track.AlbumId),
       (SELECT GenreKey FROM ChinookDW4.dbo.DimGenre WHERE GenreId = Track.GenreId),
       (SELECT MediaTypeKey FROM ChinookDW4.dbo.DimMediaType WHERE MediaTypeId = Track.MediaTypeId),
       (SELECT EmployeeKey FROM ChinookDW4.dbo.DimEmployee WHERE EmployeeId = Customer.SupportRepId),
       InvoiceLine.Quantity,
       InvoiceLine.UnitPrice,
       (InvoiceLine.Quantity * InvoiceLine.UnitPrice) AS TotalAmount
FROM InvoiceLine
JOIN Invoice ON InvoiceLine.InvoiceId = Invoice.InvoiceId
JOIN Track ON InvoiceLine.TrackId = Track.TrackId
JOIN Customer ON Invoice.CustomerId = Customer.CustomerId;

GO