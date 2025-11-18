package com.capstone.service;

import com.capstone.config.TableCreation;
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

    private final TableCreation tableCreation;
    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;
    private final SparkSession spark;

    public SparkPlanService(TableCreation tableCreation, SparkPlanExtractor extractor, SparkPlanParser parser, SparkSession sparkSession) {
        this.tableCreation = tableCreation;
        this.extractor = extractor;
        this.parser = parser;
        this.spark = sparkSession;  // Spring injects the SparkSession bean defined in SparkConfig
    }

    public QueryResponse translateSql(String sparkSql) {
        QueryResponse resp = new QueryResponse();
        List<String> warnings = new ArrayList<>();
        try {
            tableCreation.createDemoTempViews(); // Test data
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