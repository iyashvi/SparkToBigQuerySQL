package com.capstone.config;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.*;

@Configuration
public class TableCreation {

    private final SparkSession spark;

    public TableCreation(SparkSession spark) {
        this.spark = spark;
    }

    public void createDemoTempViews() {
        // Employees table with EmployeeID
        spark.sql(
                "CREATE OR REPLACE TEMP VIEW Employees AS " +
                        "SELECT 1 AS EmployeeID, 'John Doe' AS Name, 32 AS Age UNION ALL " +
                        "SELECT 2 AS EmployeeID, 'Jane Smith' AS Name, 28 AS Age UNION ALL " +
                        "SELECT 3 AS EmployeeID, 'Peter Jones' AS Name, 46 AS Age UNION ALL " +
                        "SELECT 4 AS EmployeeID, 'Alice Brown' AS Name, 35 AS Age"
        );


        // Department table with EmployeeID to match Employees
        spark.sql(
                "CREATE OR REPLACE TEMP VIEW Department AS " +
                        "SELECT 1 AS EmployeeID, 'Sales' AS Dept, 52000.0 AS Salary UNION ALL " +
                        "SELECT 2 AS EmployeeID, 'Marketing' AS Dept, 45000.0 AS Salary UNION ALL " +
                        "SELECT 3 AS EmployeeID, 'HR' AS Dept, 60000.0 AS Salary UNION ALL " +
                        "SELECT 4 AS EmployeeID, 'Engineering' AS Dept, 75000.0 AS Salary"
        );

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW Customers AS " +
                        "SELECT 1 AS CustomerID, 'Alice' AS CustomerName, 'New York' AS City UNION ALL " +
                        "SELECT 2 AS CustomerID, 'Bob' AS CustomerName, 'Los Angeles' AS City UNION ALL " +
                        "SELECT 3 AS CustomerID, 'Charlie' AS CustomerName, 'Chicago' AS City UNION ALL " +
                        "SELECT 4 AS CustomerID, 'David' AS CustomerName, 'Houston' AS City"
        );

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW Orders AS " +
                        "SELECT 1 AS OrderID, 1 AS CustomerID, '2023-10-01' AS OrderDate, 250.0 AS TotalAmount, " +
                        "ARRAY('Laptop', 'Mouse', 'Keyboard') AS Items, " +
                        "MAP('ProductID', 101, 'Quantity', 1, 'Price', 250.0) AS OrderDetails " +
                        "UNION ALL " +
                        "SELECT 2 AS OrderID, 2 AS CustomerID, '2023-09-15' AS OrderDate, 450.0 AS TotalAmount, " +
                        "ARRAY('Phone', 'Charger') AS Items, " +
                        "MAP('ProductID', 102, 'Quantity', 2, 'Price', 225.0) AS OrderDetails " +
                        "UNION ALL " +
                        "SELECT 3 AS OrderID, 3 AS CustomerID, '2023-08-20' AS OrderDate, 120.0 AS TotalAmount, " +
                        "ARRAY('Headphones') AS Items, " +
                        "MAP('ProductID', 103, 'Quantity', 1, 'Price', 120.0) AS OrderDetails " +
                        "UNION ALL " +
                        "SELECT 4 AS OrderID, 4 AS CustomerID, '2023-07-30' AS OrderDate, 300.0 AS TotalAmount, " +
                        "ARRAY('Monitor', 'Desk Lamp') AS Items, " +
                        "MAP('ProductID', 104, 'Quantity', 1, 'Price', 300.0) AS OrderDetails"
        );
    }
}
