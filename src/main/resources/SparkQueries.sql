SELECT
    e.EmployeeID,
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge,
    d.Dept AS DepartmentName,
    d.Salary,
    RANK() OVER (PARTITION BY d.Dept ORDER BY d.Salary DESC) AS DeptRankBySalary,
    DENSE_RANK() OVER (PARTITION BY d.Dept ORDER BY e.Age ASC) AS DeptRankByAge,
    ROW_NUMBER() OVER (ORDER BY d.Salary DESC) AS GlobalRowNumber,
    SUM(d.Salary) OVER (PARTITION BY d.Dept) AS TotalDeptSalary,
    AVG(d.Salary) OVER (PARTITION BY d.Dept) AS AvgDeptSalary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
WHERE
    e.Age > 25
GROUP BY
    e.EmployeeID, e.Name, e.Age, d.Dept, d.Salary
HAVING
    AVG(d.Salary) > 30000
ORDER BY
    d.Salary DESC
LIMIT 10;

-- 1. SELECT
SELECT
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge
FROM
    Employees AS e;

-- 2. SELECT + WHERE
SELECT
    Name AS EmployeeName,
    Age AS EmployeeAge
FROM
    Employees
WHERE
    Age > 30;

-- 3. JOIN
SELECT
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge,
    d.Dept AS DepartmentName
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID;

-- 4. GROUP BY
SELECT
    d.Dept AS DepartmentName,
    AVG(d.Salary) AS AverageSalary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
GROUP BY
    d.Dept;

-- 5. HAVING
SELECT
    d.Dept AS DepartmentName,
    AVG(d.Salary) AS AverageSalary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
GROUP BY
    d.Dept
HAVING
    AVG(d.Salary) > 55000;

-- 6. ORDER BY
SELECT
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge
FROM
    Employees AS e
ORDER BY
    e.Age DESC;

-- 7. LIMIT
SELECT
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge
FROM
    Employees AS e
ORDER BY
    e.Age
LIMIT 2;

-- 8. SELECT with JOIN (no aliases)
SELECT
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge,
    d.Dept AS DepartmentName,
    d.Salary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID;

-- 9. WHERE + ORDER BY
SELECT
    e.Name AS EmployeeName,
    e.Age AS EmployeeAge,
    d.Dept AS DepartmentName,
    d.Salary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
WHERE
    d.Dept = 'Sales'
ORDER BY
    d.Salary DESC;

-- 10. GROUP BY + HAVING
SELECT
    e.Age AS EmployeeAge,
    COUNT(e.EmployeeID) AS NumberOfEmployees
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
GROUP BY
    e.Age
HAVING
    COUNT(e.EmployeeID) > 2;

-- 11. JOIN + GROUP BY + HAVING
SELECT
    d.Dept AS DepartmentName,
    AVG(d.Salary) AS AverageSalary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
GROUP BY
    d.Dept
HAVING
    AVG(d.Salary) > 50000;

-- 12. WHERE with JOIN
SELECT
    e.Name AS EmployeeName,
    d.Dept AS DepartmentName,
    d.Salary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
WHERE
    d.Salary > 50000;

-- 13. ORDER BY + LIMIT
SELECT
    e.Name AS EmployeeName,
    d.Dept AS DepartmentName,
    d.Salary
FROM
    Employees AS e
JOIN
    Department AS d ON e.EmployeeID = d.EmployeeID
ORDER BY
    d.Salary DESC
LIMIT 2;