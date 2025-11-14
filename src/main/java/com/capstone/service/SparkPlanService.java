package com.capstone.service;

import com.capstone.dto.QueryResponse;
import com.capstone.extractor.SparkPlanExtractor;
import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanWalker;
import com.capstone.parser.SparkPlanParser;
import com.capstone.transformer.SelectConverter;
import org.springframework.stereotype.Service;
import org.apache.spark.sql.*;

import java.util.*;

@Service
public class SparkPlanService {

    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;
    private final SparkSession spark;

    public SparkPlanService(SparkPlanExtractor extractor, SparkPlanParser parser, SparkSession sparkSession) {
        this.extractor = extractor;
        this.parser = parser;
        this.spark = sparkSession;  // Spring injects the SparkSession bean defined in SparkConfig
    }

    public QueryResponse translateSql(String sparkSql) {
        QueryResponse resp = new QueryResponse();
        List<String> warnings = new ArrayList<>();
        try {
            spark.sql(
                    "CREATE OR REPLACE TEMP VIEW Employees AS " +
                            "SELECT 1 AS EmployeeID, 'John Doe' AS Name, 32 AS Age " +
                            "UNION ALL SELECT 2 AS EmployeeID , 'Jane Smith' AS Name, 28 AS Age " +
                            "UNION ALL SELECT 3 AS EmployeeID, 'Peter Jones' AS Name, 46 AS Age " +
                            "UNION ALL SELECT 4 AS EmployeeID, 'Alice Brown' AS Name, 35 AS Age "
            );

            spark.sql(
                    "CREATE OR REPLACE TEMP VIEW Department AS " +
                            "SELECT 1 AS EmployeeID, 'Sales' AS Dept, 52000.0 AS Salary " +
                            "UNION ALL SELECT 2 AS EmployeeID,  'Marketing' AS Dept, 45000.0 AS Salary " +
                            "UNION ALL SELECT 3 AS EmployeeID,  'HR' AS Dept, 60000.0 AS Salary " +
                            "UNION ALL SELECT 4 AS EmployeeID, 'Engineering' AS Dept, 75000.0 AS Salary "
            );

            spark.sql(sparkSql);

            String logical = "";
            try {
                logical = extractor.extractLogicalPlan(sparkSql);
                String optimized = extractor.extractOptimizedPlan(sparkSql);
                String physical = extractor.extractPhysicalPlan(sparkSql);

                resp.setLogicalPlanText(logical);
                resp.setOptimizedPlanText(optimized);
                resp.setPhysicalPlanText(physical);
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