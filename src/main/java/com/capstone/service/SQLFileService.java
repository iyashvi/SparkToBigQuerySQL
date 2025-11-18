package com.capstone.service;

import com.capstone.config.TableCreation;
import com.capstone.dto.FileQueryResponse;
import com.capstone.extractor.SparkPlanExtractor;
import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanWalker;
import com.capstone.parser.SparkPlanParser;
import com.capstone.transformer.SelectConverter;
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

    private final TableCreation tableCreation;
    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;
    private final SparkSession spark;

    public SQLFileService(TableCreation tableCreation, SparkPlanExtractor extractor, SparkPlanParser parser, SparkSession sparkSession) {
        this.tableCreation = tableCreation;
        this.extractor = extractor;
        this.parser = parser;
        this.spark = sparkSession;  // Spring injects the SparkSession bean defined in SparkConfig
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

        tableCreation.createDemoTempViews(); // Test data

        for (String query : queries) {
            String trimmedQuery = query.trim();
            if (!trimmedQuery.isEmpty()) {
                responses.add(translateSql(trimmedQuery));
            }
        }
        return responses;
    }

    public FileQueryResponse translateSql(String sparkSql) {
        FileQueryResponse resp = new FileQueryResponse();
        List<String> warnings = new ArrayList<>();
        try {
            spark.sql(sparkSql);

            String logical = "";
            try {
                logical = extractor.extractLogicalPlan(sparkSql);
                resp.setLogicalPlanText(logical);
            }
            catch (Exception e) {
                warnings.add("Error extracting Spark plans: " + e.getMessage());
            }

            System.out.println("Logical Plan ================= " + logical);
            SparkPlanNode root = parser.parse(logical);

            if (root == null) {
                throw new IllegalStateException("Parsed plan is empty — no valid root node found.");
            }
            // 3. Walk nodes using visitor
            PlanWalker walker = new PlanWalker();
            SelectConverter converter = new SelectConverter();
            walker.walk(root, converter);

            // 4. Return transformed query
            String bigQuerySql = converter.getQuery();
            System.out.println("BigQuery ================= " + bigQuerySql);

            resp.setBigQuerySql(bigQuerySql);
        }
        catch (Exception e) {
            warnings.add("Error while analyzing query: " + e.getMessage());
            resp.setBigQuerySql("/* translation failed: " + e.getMessage() + " */");
        }

        resp.setWarnings(warnings);
        return resp;
    }
}
