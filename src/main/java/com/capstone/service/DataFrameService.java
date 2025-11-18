package com.capstone.service;

import com.capstone.dto.QueryResponse;
import com.capstone.extractor.SparkPlanExtractor;
import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanWalker;
import com.capstone.parser.SparkPlanParser;
import com.capstone.transformer.SelectConverter;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class DataFrameService {

    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;
    private final SparkSession spark;

    public DataFrameService(SparkPlanExtractor extractor, SparkPlanParser parser, SparkSession spark) {
        this.extractor = extractor;
        this.parser = parser;
        this.spark = spark;
    }

    public QueryResponse evaluateDataFrame(String dfCode) {
        QueryResponse resp = new QueryResponse();
        List<String> warnings = new ArrayList<>();

        try {
            if (dfCode == null || dfCode.isBlank()) {
                resp.setBigQuerySql("/* Empty DataFrame code */");
                return resp;
            }

            // STEP 1: Convert to Spark SQL
            String sparkSql = convertDataFrameToSql(dfCode);
            System.out.println("Generated Spark SQL: " + sparkSql);

            // STEP 2: Provide test data
            createDemoTempViews();

            // STEP 3: Validate SQL
            spark.sql(sparkSql);

            // STEP 4: Extract plans
            String logical = extractor.extractLogicalPlan(sparkSql);
            resp.setLogicalPlanText(logical);
            resp.setOptimizedPlanText(extractor.extractOptimizedPlan(sparkSql));
            resp.setPhysicalPlanText(extractor.extractPhysicalPlan(sparkSql));

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

    private void createDemoTempViews() {
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

