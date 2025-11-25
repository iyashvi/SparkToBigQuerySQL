SELECT
    COUNT(ProductID) AS TotalProducts,
    MIN(ProductID) AS MinProductID,
    MAX(ProductID) AS MaxProductID,
    SUM(ProductID) AS SumOfProductIDs,
    AVG(ProductID) AS AvgProductID,
    
    MIN(CAST(P.CreationDate AS DATE)) AS EarliestProductCreationDate,
    MAX(CAST(P.CreationDate AS DATE)) AS LatestProductCreationDate,
    
    AVG(LENGTH(P.Name)) AS AvgProductNameLength,
    MAX(LENGTH(P.Name)) AS MaxProductNameLength,
    MIN(LENGTH(P.Name)) AS MinProductNameLength,
    SUM(LENGTH(P.Name)) AS TotalProductNameDataLength,
    MAX(LENGTH(ENCODE(P.Name, 'UTF-8'))) AS MaxProductNameBytes,
	MIN(LENGTH(ENCODE(P.Name, 'UTF-8'))) AS MinProductNameBytes,

    COUNT(DISTINCT P.CategoryID) AS DistinctCategories,
    COUNT(DISTINCT P.SupplierID) AS DistinctSuppliers,

    CURRENT_TIMESTAMP() AS CurrentDateTime, 
    CURRENT_DATE() AS CurrentDate,         
    FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'UTC') AS CurrentUTCDate,
    
    YEAR(CURRENT_DATE()) AS CurrentYear,
    DATE_FORMAT(CURRENT_DATE(), 'MMMM') AS CurrentMonthName,

    nvl(P.Name, 'Unknown Product') AS ProductName,
    LENGTH(COALESCE(P.Name, 'Unknown Product')) AS ProductNameLength,
    LENGTH(ENCODE(COALESCE(P.Name, 'Unknown Product'), 'UTF-8')) AS ProductNameByteSize,

    DATE_ADD(CURRENT_DATE(), 30) AS NextStockUpdateDate,
    DATEDIFF(CURRENT_DATE(), CAST(P.CreationDate AS DATE)) AS DaysSinceProductLaunch,
    MONTHS_BETWEEN(CURRENT_DATE(), CAST(P.CreationDate AS DATE)) AS MonthsSinceProductLaunch,
    FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), CAST(P.CreationDate AS DATE)) / 12) AS YearsSinceProductLaunch,
    CAST(P.CreationDate AS DATE) AS ProductLaunchDate,
    
    DATE_FORMAT(P.CreationDate, 'yyyy-MM-dd') AS ProductLaunchDateFormatted,

    ROUND(AVG(P.Price), 2) AS AvgPrice,
    SUM(P.Price) AS TotalPrice,
    MIN(P.Price) AS MinPrice,
    MAX(P.Price) AS MaxPrice,
    STDDEV(P.Price) AS PriceStandardDeviation,
    VAR_POP(P.Price) AS PriceVariance, 
    CEIL(AVG(P.Price)) AS CeilAvgPrice,   
    FLOOR(AVG(P.Price)) AS FloorAvgPrice, 
    POWER(AVG(CAST(P.Price AS FLOAT)), 2) AS AvgPriceSquared,
    ABS(MIN(P.Price)) AS AbsoluteMinPrice,

    UUID() AS UniqueRequestID, 
    HASH(P.Name, P.ProductID, P.CreationDate) AS RecordChecksum,
    SUBSTRING(P.Name, 1, 5) AS ProductPrefix,
    SUBSTRING(P.Name, LENGTH(P.Name) - 4, 5) AS ProductSuffix,
    INSTR(P.Name, 'Pro') AS PositionOfPro,
    REPLACE(P.Name, 'Pro', 'Item') AS ReplacedName, 
    UPPER(P.Name) AS UppercaseName,               
    LOWER(P.Name) AS LowercaseName,              
    REVERSE(P.Name) AS ReversedName               
FROM
    Product AS P
WHERE 
P.ProductID = 101
GROUP BY
    P.Name,
    P.ProductID,
    P.CreationDate,
    P.CategoryID,
    P.SupplierID,
    P.Price
;