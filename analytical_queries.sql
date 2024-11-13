SELECT 
    DimCustomer.Country AS Region,
    COUNT(DISTINCT FactSales.CustomerKey) AS NumberOfCustomers,
    SUM(FactSales.TotalAmount) AS TotalSpending,
    AVG(FactSales.TotalAmount) AS AverageOrderValue
FROM 
    FactSales
JOIN 
    DimCustomer ON FactSales.CustomerKey = DimCustomer.CustomerKey
GROUP BY 
    DimCustomer.Country
ORDER BY 
    TotalSpending DESC;



SELECT 
    DimTrack.Name AS TrackName,
    SUM(FactSales.Quantity) AS QuantitySold,
    SUM(FactSales.TotalAmount) AS Revenue
FROM 
    FactSales
JOIN 
    DimTrack ON FactSales.TrackKey = DimTrack.TrackKey
JOIN 
    DimDate ON FactSales.DateKey = DimDate.DateKey
WHERE 
    DimDate.Year = 2023 -- Specify the desired year or range
GROUP BY 
    DimTrack.Name
ORDER BY 
    Revenue DESC; -- Or use `QuantitySold DESC` for top quantity

