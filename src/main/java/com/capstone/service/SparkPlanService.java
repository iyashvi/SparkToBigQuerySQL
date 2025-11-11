package com.capstone.service;

import com.capstone.dto.QueryResponse;
//import com.capstone.parser.PlanParser;
//import com.capstone.transformer.PlanToBigQueryTransformer;
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
    private final SelectConverter selectConverter;
    private SparkSession spark;

    public SparkPlanService(SparkPlanExtractor extractor, SparkPlanParser parser, SelectConverter selectConverter) {
        this.extractor = extractor;
        this.parser = parser;
        this.selectConverter = selectConverter;
    }

    private synchronized SparkSession getSparkSession() {
        if (spark == null) {
            spark = SparkSession.builder()
                    .appName("spark-to-bigquery-local")
                    .master("local[*]")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
        }
        return spark;
    }

    public QueryResponse translateSql(String sparkSql) {
        QueryResponse resp = new QueryResponse();
        List<String> warnings = new ArrayList<>();
        try {
            Dataset<Row> df = getSparkSession().sql(sparkSql);

            String logical = "";
            String optimized = "";
            String physical = "";
            try {
                Object qe = df.getClass().getMethod("queryExecution").invoke(df);
                Object logicalPlan = qe.getClass().getMethod("logical").invoke(qe);
                Object analyzed = qe.getClass().getMethod("analyzed").invoke(qe);
                Object optimizedPlan = qe.getClass().getMethod("optimizedPlan").invoke(qe);
                Object executed = qe.getClass().getMethod("executedPlan").invoke(qe);
                logical = logicalPlan.toString();
                optimized = optimizedPlan.toString();
                physical = executed.toString();
            } catch (Exception e) {
                warnings.add("Could not call queryExecution() reflectively — falling back to explain(). For full fidelity, run with matching Spark runtime on classpath.");
                logical = df.queryExecution().logical().toString();
                optimized = df.queryExecution().optimizedPlan().toString();
                physical = df.queryExecution().executedPlan().toString();
            }

            resp.setLogicalPlanText(logical);
            resp.setOptimizedPlanText(optimized);
            resp.setPhysicalPlanText(physical);

            SparkPlanNode root = parser.parse(logical);

            // 3. Walk nodes using visitor
            PlanWalker walker = new PlanWalker();
            walker.walk(root, selectConverter);

            // 4. Return transformed query
            String bigQuerySql = selectConverter.getQuery();
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