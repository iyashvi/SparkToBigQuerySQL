package com.capstone.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("SparkToBigQueryCapstone")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }
}
