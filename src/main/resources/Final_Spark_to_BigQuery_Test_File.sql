-- ===========================================================
-- 1. Simple Select with String Literal Containing Space
-- ===========================================================
SELECT *
FROM Employees
WHERE Name = 'John Doe'
  AND Age = 12;

-- ===========================================================
-- 2. Basic Select with Join and WHERE clause
-- ===========================================================
SELECT e.EmployeeID, e.Name, e.Age, e.Dept, e.Salary, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30
ORDER BY o.TotalAmount DESC
LIMIT 5 OFFSET 0;

-- ===========================================================
-- 3. Group By with Aggregation (SUM)
-- ===========================================================
SELECT e.Dept, COUNT(o.OrderID) AS TotalOrders, SUM(o.TotalAmount) AS TotalSales
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
GROUP BY e.Dept
HAVING SUM(o.TotalAmount) > 200;

-- ===========================================================
-- 4. Using COALESCE and NVL for Null Handling
-- ===========================================================
SELECT e.EmployeeID, e.Name,
       COALESCE(o.TotalAmount, 0) AS OrderAmount,
       NVL(o.OrderDate, '2023-01-01') AS OrderDate
FROM Employees e
LEFT JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30;

-- ===========================================================
-- 5. Using TO_DATE function to convert string to date
-- ===========================================================
SELECT o.OrderID, TO_DATE(o.OrderDate) AS OrderDateFormatted
FROM Orders o
WHERE TO_DATE(o.OrderDate) >= '2023-08-01';

-- ===========================================================
-- 6. Using EXPLODE function (Array Expansion)
-- ===========================================================
SELECT o.OrderID, item
FROM Orders o
LATERAL VIEW EXPLODE(o.Items) AS item;

-- ===========================================================
-- 7. Using STRUCT to create a nested structure
-- ===========================================================
SELECT e.EmployeeID, STRUCT(e.Name, e.Age, e.Salary) AS EmployeeDetails
FROM Employees e
WHERE e.Age > 30;

-- ===========================================================
-- 8. Using MAP_KEYS function to extract keys from a MAP column
-- ===========================================================
SELECT o.OrderID, MAP_KEYS(o.OrderDetails) AS ProductKeys
FROM Orders o;

-- ===========================================================
-- 9. Using CAST for Decimal Type
-- ===========================================================
SELECT o.OrderID, CAST(o.TotalAmount AS DECIMAL(10, 2)) AS PreciseAmount
FROM Orders o
WHERE o.TotalAmount > 100;

-- ===========================================================
-- 10. Using CASE WHEN for conditional logic
-- ===========================================================
SELECT e.EmployeeID, e.Name,
       CASE
           WHEN e.Age < 30 THEN 'Young'
           WHEN e.Age BETWEEN 30 AND 40 THEN 'Mid-Aged'
           ELSE 'Senior'
       END AS AgeCategory
FROM Employees e;

-- ===========================================================
-- 11. CASE WHEN with String Literals Containing Spaces
-- ===========================================================
SELECT e.EmployeeID,
       e.Name,
       CASE
           WHEN e.Age < 30 THEN 'Young Person'
           WHEN e.Age BETWEEN 30 AND 40 THEN 'Mid-Aged'
           ELSE 'Senior Person'
       END AS AgeCategory
FROM Employees e;

-- ===========================================================
-- 12. Using Query Hint (e.g., Broadcast Join)
-- ===========================================================
SELECT /*+ BROADCASTJOIN(o) */ e.EmployeeID, e.Name, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30;

-- ===========================================================
-- 13. Using LIMIT and OFFSET for Pagination
-- ===========================================================
SELECT e.EmployeeID, e.Name, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
ORDER BY o.TotalAmount DESC
LIMIT 5 OFFSET 10;

-- ===========================================================
-- 14. Nested Query for Aggregation
-- ===========================================================
SELECT o.CustomerID, MAX(o.TotalAmount) AS MaxOrderAmount
FROM Orders o
GROUP BY o.CustomerID
HAVING MAX(o.TotalAmount) > 200;

-- ===========================================================
-- 15. LEFT JOIN to handle NULLs in orders
-- ===========================================================
SELECT e.EmployeeID, e.Name, o.OrderID, o.TotalAmount
FROM Employees e
LEFT JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 30;

-- ===========================================================
-- 16. Aggregation with HAVING clause
-- ===========================================================
SELECT o.CustomerID, COUNT(o.OrderID) AS TotalOrders
FROM Orders o
GROUP BY o.CustomerID
HAVING COUNT(o.OrderID) > 1;

-- ===========================================================
-- 17. Using CAST for String-to-Date Conversion
-- ===========================================================
SELECT o.OrderID, CAST(o.OrderDate AS DATE) AS OrderDate
FROM Orders o
WHERE CAST(o.OrderDate AS DATE) > '2023-01-01';

-- ===========================================================
-- 18. Using WHERE with Date and String Functions
-- ===========================================================
SELECT o.OrderID, o.OrderDate
FROM Orders o
WHERE TO_DATE(o.OrderDate) BETWEEN '2023-01-01' AND '2023-10-01'
  AND LOWER(o.Items[0]) LIKE '%laptop%';

-- ===========================================================
-- 19. ROW_NUMBER() for Ranking Orders Per Customer
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.TotalAmount DESC) AS RowNum
FROM Orders o;

-- ===========================================================
-- 20. RANK() for Handling Ties in Order Amounts
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       RANK() OVER (PARTITION BY o.CustomerID ORDER BY o.TotalAmount DESC) AS RankNum
FROM Orders o;

-- ===========================================================
-- 21. DENSE_RANK() for Ranking Without Gaps
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       DENSE_RANK() OVER (PARTITION BY o.CustomerID ORDER BY o.TotalAmount DESC) AS DenseRankNum
FROM Orders o;

-- ===========================================================
-- 22. SUM() OVER() for Running Totals
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       SUM(o.TotalAmount) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningTotal
FROM Orders o;

-- ===========================================================
-- 23. AVG() OVER() for Moving Average
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       AVG(o.TotalAmount) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderID ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MovingAvg
FROM Orders o;

-- ===========================================================
-- 24. FIRST_VALUE() and LAST_VALUE()
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       FIRST_VALUE(o.TotalAmount) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderID) AS FirstOrderAmount,
       LAST_VALUE(o.TotalAmount) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastOrderAmount
FROM Orders o;

-- ===========================================================
-- 25. NTILE() for Quartiles
-- ===========================================================
SELECT o.CustomerID, o.OrderID, o.TotalAmount,
       NTILE(4) OVER (PARTITION BY o.CustomerID ORDER BY o.TotalAmount DESC) AS Quartile
FROM Orders o;

-- ===========================================================
-- 26. Spark SQL with multiple window functions and aggregations
-- ===========================================================
SELECT e.EmployeeID,
       e.Name,
       e.Age + 10 AS AgePlusTen,
       e.Dept,
       NVL(e.Salary, 100) AS SalaryWithDefault,
       RANK() OVER (PARTITION BY e.Dept ORDER BY e.Salary DESC) AS salaryRanking,
       DENSE_RANK() OVER (PARTITION BY e.Dept ORDER BY e.Age ASC) AS ageRanking,
       ROW_NUMBER() OVER (ORDER BY e.Salary DESC) AS departmentRowNum,
       SUM(e.Salary) OVER (PARTITION BY e.Dept) AS rollingTotalSalary,
       AVG(e.Salary) AS averageSalary
FROM Employees AS e
JOIN Orders o ON e.EmployeeID = o.CustomerID
WHERE e.Age > 25
  AND e.Dept = 'IT'
GROUP BY e.EmployeeID, e.Name, e.Age, e.Dept, e.Salary
HAVING AVG(e.Salary) > 30000
ORDER BY e.Salary DESC
LIMIT 10 OFFSET 5;

-- ===========================================================
-- 27. Complex Join with Multiple Tables (Employees, Orders, Customers)
-- ===========================================================
SELECT e.EmployeeID, e.Name, c.CustomerID, c.CustomerName, o.OrderID, o.TotalAmount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.CustomerID
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE e.Age > 30 AND o.TotalAmount > 100
ORDER BY o.TotalAmount DESC;