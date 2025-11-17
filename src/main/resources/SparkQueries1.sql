SELECT 
Employees.EmployeeID, Employees.Name, Employees.Age, Department.Dept, Department.Salary, 
RANK() OVER
(PARTITION BY Department.Dept ORDER BY Department.Salary DESC), 
DENSE_RANK() OVER 
(PARTITION BY Department.Dept ORDER BY Employees.Age ASC), 
ROW_NUMBER() OVER (ORDER BY Department.Salary DESC), 
SUM(Department.Salary) OVER (PARTITION BY Department.Dept), 
AVG(Department.Salary) 
FROM Employees 
JOIN Department 
ON Employees.EmployeeID = Department.EmployeeID 
WHERE Employees.Age > 25 
GROUP BY Employees.EmployeeID, Employees.Name, Employees.Age, Department.Dept, Department.Salary 
HAVING AVG(Department.Salary) > 30000 
ORDER BY Department.Salary DESC 
LIMIT 10;

-- 1. SELECT
SELECT Name, Age
FROM Employees;

-- 2. SELECT + WHERE
SELECT Name, Age
FROM Employees
WHERE Age > 30;

-- 3. JOIN
SELECT Employees.Name, Employees.Age, Department.Dept
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID;

-- 4. GROUP BY
SELECT Department.Dept, AVG(Department.Salary)
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
GROUP BY Department.Dept;

-- 5. HAVING
SELECT Department.Dept, AVG(Department.Salary)
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
GROUP BY Department.Dept
HAVING AVG(Department.Salary) > 55000;

-- 6. ORDER BY
SELECT Name, Age
FROM Employees
ORDER BY Age DESC;

-- 7. LIMIT
SELECT Name, Age
FROM Employees
ORDER BY Age
LIMIT 2;

-- 8. SELECT with JOIN (no aliases)
SELECT Employees.Name, Employees.Age, Department.Dept, Department.Salary
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID;

-- 9. WHERE + ORDER BY
SELECT Employees.Name, Employees.Age, Department.Dept, Department.Salary
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
WHERE Department.Dept = 'Sales'
ORDER BY Department.Salary DESC;

-- 10. GROUP BY + HAVING (no aliases)
SELECT Employees.Age, COUNT(Employees.EmployeeID)
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
WHERE Employees.Age > 30
GROUP BY Employees.Age
HAVING COUNT(Employees.EmployeeID) > 1;

-- 11. JOIN + GROUP BY + HAVING
SELECT Department.Dept, AVG(Department.Salary)
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
GROUP BY Department.Dept
HAVING AVG(Department.Salary) > 50000;

-- 12. WHERE with JOIN
SELECT Employees.Name, Department.Dept, Department.Salary
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
WHERE Department.Salary > 50000;

-- 13. ORDER BY + LIMIT
SELECT Employees.Name, Department.Dept, Department.Salary
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
ORDER BY Department.Salary DESC
LIMIT 2;

-- 14. Subquery (no aliases)
SELECT Employees.Name, Employees.Age, Department.Salary
FROM Employees
JOIN Department
  ON Employees.EmployeeID = Department.EmployeeID
WHERE Department.Salary > (
    SELECT AVG(Salary)
    FROM Department
);

-- 15. GROUP BY + HAVING with subquery in FROM (no aliases)
SELECT Dept, AVG(Salary)
FROM (
    SELECT Department.Dept, Department.Salary
    FROM Employees
    JOIN Department
      ON Employees.EmployeeID = Department.EmployeeID
)
GROUP BY Dept
HAVING AVG(Salary) > 50000;
