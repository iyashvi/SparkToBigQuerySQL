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
    }
}
