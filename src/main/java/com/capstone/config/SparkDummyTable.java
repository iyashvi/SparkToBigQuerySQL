package com.capstone.config;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;

public class SparkDummyTable {

    public static void registerDummyTables(SparkSession spark) {
        // Define dummy data
        List<Row> data = Arrays.asList(
                RowFactory.create("Alice", 25),
                RowFactory.create("Bob", 30),
                RowFactory.create("Charlie", 35)
        );

        // Define schema
        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);

        // Create DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Register as temporary view
        df.createOrReplaceTempView("employees");

        System.out.println("✅ Dummy table 'employees' registered successfully!");
    }
}
