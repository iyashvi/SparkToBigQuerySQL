package com.capstone.service;

import com.capstone.config.TableCreation;
import com.capstone.dto.FileQueryResponse;
import com.capstone.dto.QueryResponse;
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
import java.util.Arrays;
import java.util.List;

@Service
public class DfFileService {

    private static final Logger log = LoggerFactory.getLogger(DfFileService.class);

    private final TableCreation tableCreation;
    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;
    private final SparkSession spark;

    public DfFileService(TableCreation tableCreation, SparkPlanExtractor extractor, SparkPlanParser parser, SparkSession spark) {
        this.tableCreation = tableCreation;
        this.extractor = extractor;
        this.parser = parser;
        this.spark = spark;
    }

    // Helper to read SQL file content into String
    public static String readDfFile(String filePath) throws IOException {
        log.info("Reading DF file content from: {}", filePath);
        try {
            return Files.readString(Paths.get(filePath));
        } catch (IOException e) {
            log.error("FATAL: Could not read DF file at {}.", filePath, e);
            throw new UncheckedIOException("Failed to read DF file: " + filePath, e);
        }
    }

    public List<FileQueryResponse> extractQueriesFromFile(String filePath) throws IOException {
        log.info("DF file path: {}", filePath);

        List<FileQueryResponse> responses = new ArrayList<>();
        String fileContent = readDfFile(filePath);
        String[] queries = fileContent.split("(?=spark\\.table\\()");

        tableCreation.createDemoTempViews(); // Test data

        for (String query : queries) {
            String trimmedQuery = query.trim();
            if (!trimmedQuery.isEmpty()) {
                responses.add(evaluateDataFrame(trimmedQuery));
            }
        }
        return responses;
    }

    public FileQueryResponse evaluateDataFrame(String dfCode) {
        FileQueryResponse resp = new FileQueryResponse();
        List<String> warnings = new ArrayList<>();

        try {
            if (dfCode == null || dfCode.isBlank()) {
                resp.setBigQuerySql("/* Empty DataFrame code */");
                return resp;
            }

            // STEP 1: Convert to Spark SQL
            String sparkSql = convertDataFrameToSql(dfCode.replace("\\", ""));
            System.out.println("Generated Spark SQL: " + sparkSql);

            tableCreation.createDemoTempViews(); // Test data
            spark.sql(sparkSql);

            // STEP 4: Extract plans
            String logical = extractor.extractLogicalPlan(sparkSql);
            resp.setLogicalPlanText(logical);

            System.out.println("Logical Plan ================= " + logical);

            // STEP 5: PARSE + CONVERT
            SparkPlanNode root = parser.parse(logical);
            PlanWalker walker = new PlanWalker();
            SelectConverter converter = new SelectConverter();
            walker.walk(root, converter);

            resp.setBigQuerySql(converter.getQuery());
        }
        catch (Exception e) {
            warnings.add("Error while evaluating DataFrame: " + e.getMessage());
            resp.setBigQuerySql("/* translation failed: " + e.getMessage() + " */");
        }

        resp.setWarnings(warnings);
        return resp;
    }

    private String convertDataFrameToSql(String dfCode) {

        dfCode = dfCode.replace("\n", " ").trim();

        String baseTable = null;
        String select = "*";
        String where = null;
        String having = null;
        String order = null;
        String limit = null;

        List<String> groupBy = new ArrayList<>();
        List<String> agg = new ArrayList<>();

        String[] parts = dfCode.split("\\.");



        for (String p : parts) {
            p = p.trim();

            if (p.startsWith("table(")) {
                baseTable = extractBalanced(p);

            } else if (p.startsWith("select(")) {
                select = extractBalanced(p);

            } else if (p.startsWith("filter(")) {
                where = extractBalanced(p);

            } else if (p.startsWith("groupBy(")) {
                groupBy = Arrays.asList(extractBalanced(p).split(","));

            } else if (p.startsWith("agg(")) {
                agg = Arrays.asList(extractBalanced(p).split(","));

            } else if (p.startsWith("having(")) {
                having = extractBalanced(p);

            } else if (p.startsWith("orderBy(")) {
                order = extractBalanced(p);

            } else if (p.startsWith("limit(")) {
                limit = extractBalanced(p);

            }
        }
        // BUILD SQL

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(!agg.isEmpty() ? String.join(", ", agg) : select);
        sql.append(" FROM ").append(baseTable);


        if (where != null) sql.append(" WHERE ").append(where);
        if (!groupBy.isEmpty()) sql.append(" GROUP BY ").append(String.join(", ", groupBy));
        if (having != null) sql.append(" HAVING ").append(having);
        if (order != null) sql.append(" ORDER BY ").append(order);
        if (limit != null) sql.append(" LIMIT ").append(limit);

        sql.append(";");

        return sql.toString();
    }

    private String extractBalanced(String expr) {
        int start = expr.indexOf("(") + 1;
        int end = findClosingParen(expr, start - 1);
        if (start < 0 || end < 0 || end <= start) return "";
        return expr.substring(start, end)
                .replace("\"", "")
                .replace("'", "")
                .trim();
    }

    private int findClosingParen(String str, int openPos) {
        int depth = 0;
        for (int i = openPos; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            if (depth == 0) return i;
        }
        return -1;
    }
}
