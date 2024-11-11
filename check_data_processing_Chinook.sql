-- SQL script to verify the data cleanup performed by the Python script

USE Chinook;
GO

-- Check for duplicate rows in each table
WITH CTE AS (
    SELECT AlbumId, COUNT(*) AS cnt
    FROM Album
    GROUP BY AlbumId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT ArtistId, COUNT(*) AS cnt
    FROM Artist
    GROUP BY ArtistId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT CustomerId, COUNT(*) AS cnt
    FROM Customer
    GROUP BY CustomerId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT EmployeeId, COUNT(*) AS cnt
    FROM Employee
    GROUP BY EmployeeId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT GenreId, COUNT(*) AS cnt
    FROM Genre
    GROUP BY GenreId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT InvoiceId, COUNT(*) AS cnt
    FROM Invoice
    GROUP BY InvoiceId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT InvoiceLineId, COUNT(*) AS cnt
    FROM InvoiceLine
    GROUP BY InvoiceLineId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT MediaTypeId, COUNT(*) AS cnt
    FROM MediaType
    GROUP BY MediaTypeId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT PlaylistId, COUNT(*) AS cnt
    FROM Playlist
    GROUP BY PlaylistId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT PlaylistId, TrackId, COUNT(*) AS cnt
    FROM PlaylistTrack
    GROUP BY PlaylistId, TrackId
)
SELECT * FROM CTE WHERE cnt > 1;

WITH CTE AS (
    SELECT TrackId, COUNT(*) AS cnt
    FROM Track
    GROUP BY TrackId
)
SELECT * FROM CTE WHERE cnt > 1;

-- Check for NOT NULL constraint enforcement
SELECT * FROM Album WHERE Title IS NULL OR ArtistId IS NULL;
SELECT * FROM Customer WHERE FirstName IS NULL OR LastName IS NULL OR Email IS NULL;
SELECT * FROM Employee WHERE LastName IS NULL OR FirstName IS NULL;
SELECT * FROM Track WHERE Name IS NULL OR MediaTypeId IS NULL OR Milliseconds <= 0 OR UnitPrice < 0;

-- Check for foreign key consistency
SELECT * FROM Album WHERE ArtistId NOT IN (SELECT ArtistId FROM Artist);
SELECT * FROM Customer WHERE SupportRepId IS NOT NULL AND SupportRepId NOT IN (SELECT EmployeeId FROM Employee);
SELECT * FROM Invoice WHERE CustomerId NOT IN (SELECT CustomerId FROM Customer);
SELECT * FROM InvoiceLine WHERE InvoiceId NOT IN (SELECT InvoiceId FROM Invoice) OR TrackId NOT IN (SELECT TrackId FROM Track);
SELECT * FROM PlaylistTrack WHERE PlaylistId NOT IN (SELECT PlaylistId FROM Playlist) OR TrackId NOT IN (SELECT TrackId FROM Track);
SELECT * FROM Track WHERE AlbumId IS NOT NULL AND AlbumId NOT IN (SELECT AlbumId FROM Album) OR GenreId IS NOT NULL AND GenreId NOT IN (SELECT GenreId FROM Genre) OR MediaTypeId NOT IN (SELECT MediaTypeId FROM MediaType);

-- Check for whitespace trimming in string columns
SELECT * FROM Customer WHERE FirstName LIKE ' %' OR FirstName LIKE '% ';
SELECT * FROM Employee WHERE LastName LIKE ' %' OR LastName LIKE '% ';
SELECT * FROM Track WHERE Name LIKE ' %' OR Name LIKE '% ';

-- Check for data type and range validation
SELECT * FROM Track WHERE Milliseconds <= 0 OR UnitPrice < 0;
SELECT * FROM Invoice WHERE Total < 0;
SELECT * FROM InvoiceLine WHERE Quantity <= 0 OR UnitPrice < 0;

-- Check for standardized country names
SELECT DISTINCT Country FROM Customer WHERE Country IN ('United States', 'US', 'UK', 'GB');
