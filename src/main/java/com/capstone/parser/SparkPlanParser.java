package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import org.springframework.stereotype.Component;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString) {
        // Example Spark logical plan:
        // "Project [name#0, age#1]\n+- Relation [name, age] employees"

        String[] lines = planString.split("\n");
        SparkPlanNode root = null;

        for (String line : lines) {
            line = line.trim();

            if (line.startsWith("Project")) {
                String expr = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
                root = new SparkPlanNode("SELECT", expr);
            } else if (line.startsWith("Relation")) {
                String table = line.substring(line.lastIndexOf(" ") + 1);
                SparkPlanNode child = new SparkPlanNode("FROM", table);
                if (root != null) root.addChild(child);
            }
        }

        return root;
    }
}
