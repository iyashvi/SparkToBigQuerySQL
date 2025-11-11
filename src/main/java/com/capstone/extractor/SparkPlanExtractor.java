package com.capstone.extractor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
public class SparkPlanExtractor {

    private final SparkSession spark;

    public SparkPlanExtractor(SparkSession spark) {
        this.spark = spark;
    }

    public String extractLogicalPlan(String query) {
        Dataset<Row> df = spark.sql(query);
        return df.queryExecution().logical().toString();
    }
}

