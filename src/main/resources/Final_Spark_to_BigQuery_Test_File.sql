-- 1. Basic Select with Join and WHERE clause
SELECT e.EmployeeID, e.Name, e.Age, e.Dept, e.Salary, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30
ORDER BY o.TotalAmount DESC
LIMIT 5 OFFSET 0;

-- 2. Group By with Aggregation (SUM)
SELECT e.Dept, COUNT(o.OrderID) AS TotalOrders, SUM(o.TotalAmount) AS TotalSales
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
GROUP BY e.Dept
HAVING SUM(o.TotalAmount) > 200;

-- 3. Using COALESCE and NVL for Null Handling
SELECT e.EmployeeID, e.Name, COALESCE(o.TotalAmount, 0) AS OrderAmount, NVL(o.OrderDate, '2023-01-01') AS OrderDate
FROM Employees e
LEFT JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30;

-- 4. Using TO_DATE function to convert string to date
SELECT o.OrderID, TO_DATE(o.OrderDate) AS OrderDateFormatted
FROM Orders o
WHERE TO_DATE(o.OrderDate) >= '2023-08-01';

-- 5. Using EXPLODE function (Array Expansion)
SELECT o.OrderID, item
FROM Orders o
LATERAL VIEW EXPLODE(o.Items) AS item;

-- 6. Using STRUCT to create a nested structure
SELECT e.EmployeeID, STRUCT(e.Name, e.Age, e.Salary) AS EmployeeDetails
FROM Employees e
WHERE e.Age > 30;

-- 7. Using MAP_KEYS function to extract keys from a MAP column
SELECT o.OrderID, MAP_KEYS(o.OrderDetails) AS ProductKeys
FROM Orders o;

-- 8. Using CAST for Decimal Type
SELECT o.OrderID, CAST(o.TotalAmount AS DECIMAL(10, 2)) AS PreciseAmount
FROM Orders o
WHERE o.TotalAmount > 100;

-- 9. Using CASE WHEN for conditional logic
SELECT e.EmployeeID, e.Name,
       CASE
           WHEN e.Age < 30 THEN 'Young'
           WHEN e.Age BETWEEN 30 AND 40 THEN 'Mid-Aged'
           ELSE 'Senior'
       END AS AgeCategory
FROM Employees e;

-- 10. Using Query Hint (e.g., Broadcast Join)
SELECT /*+ BROADCASTJOIN(o) */ e.EmployeeID, e.Name, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30;

-- 11. Using LIMIT and OFFSET for Pagination
SELECT e.EmployeeID, e.Name, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
ORDER BY o.TotalAmount DESC
LIMIT 5 OFFSET 10;

-- 12. Using IN with Subquery
SELECT e.EmployeeID, e.Name
FROM Employees e
WHERE e.EmployeeID IN (SELECT o.CustomerID FROM Orders o WHERE o.TotalAmount > 200);

-- 13. Nested Query for Aggregation
SELECT o.CustomerID, MAX(o.TotalAmount) AS MaxOrderAmount
FROM Orders o
GROUP BY o.CustomerID
HAVING MAX(o.TotalAmount) > 200;

-- 14. LEFT JOIN to handle NULLs in orders
SELECT e.EmployeeID, e.Name, o.OrderID, o.TotalAmount
FROM Employees e
LEFT JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30;

-- 15. Aggregation with HAVING clause
SELECT o.CustomerID, COUNT(o.OrderID) AS TotalOrders
FROM Orders o
GROUP BY o.CustomerID
HAVING COUNT(o.OrderID) > 1;

-- 16. Using `CAST` for String-to-Date Conversion
SELECT o.OrderID, CAST(o.OrderDate AS DATE) AS OrderDate
FROM Orders o
WHERE CAST(o.OrderDate AS DATE) > '2023-01-01';

-- 17. Complex Join with Multiple Tables (Employees, Orders, Customers)
SELECT e.EmployeeID, e.Name, c.CustomerID, c.CustomerName, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE e.Age > 30 AND o.TotalAmount > 100
ORDER BY o.TotalAmount DESC;

-- 18. Using `WHERE` with Date and String Functions
SELECT o.OrderID, o.OrderDate
FROM Orders o
WHERE TO_DATE(o.OrderDate) BETWEEN '2023-01-01' AND '2023-10-01'
AND LOWER(o.Items[0]) LIKE '%laptop%';

-- 19. Using UNION ALL to combine results from two queries
SELECT e.EmployeeID, e.Name
FROM Employees e
WHERE e.Age > 30
UNION ALL
SELECT o.CustomerID, o.OrderDate
FROM Orders o
WHERE o.TotalAmount > 200;