package com.capstone.service;

import com.capstone.dto.FileQueryResponse;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class SQLFileService {

    private static final Logger log = LoggerFactory.getLogger(SQLFileService.class);

    private final SparkPlanService sparkPlanService;

    public SQLFileService(SparkPlanService sparkPlanService) {
        this.sparkPlanService = sparkPlanService;
    }

    // Helper to read SQL file content into String
    public static String readSqlFile(String filePath) throws IOException {
        log.info("Reading SQL file content from: {}", filePath);
        try {
            return Files.readString(Paths.get(filePath));
        } catch (IOException e) {
            log.error("FATAL: Could not read SQL file at {}.", filePath, e);
            throw new UncheckedIOException("Failed to read SQL file: " + filePath, e);
        }
    }

    public List<FileQueryResponse> extractQueriesFromFile(String filePath) throws IOException {
        log.info("SQL file path: {}", filePath);

        List<FileQueryResponse> responses = new ArrayList<>();
        String fileContent = readSqlFile(filePath);
        String[] queries = fileContent.split("(?<=;)");

        for (String query : queries) {
            String trimmedQuery = query.trim();
            if (!trimmedQuery.isEmpty()) {
                responses.add(sparkPlanService.translateSql(trimmedQuery));
            }
        }
        return responses;
    }
}
