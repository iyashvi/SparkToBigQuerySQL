package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import org.springframework.stereotype.Component;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString) {

        String[] lines = planString.split("\n");
        SparkPlanNode root = null;

        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            if (line.startsWith("'Project")) {
                // Extract between [ and ] → gives 'name, 'age
                int start = line.indexOf('[');
                int end = line.indexOf(']');
                String expr = "";
                if (start != -1 && end != -1 && end > start) {
                    expr = line.substring(start + 1, end)
                            .replace("'", "")
                            .trim();
                }
                root = new SparkPlanNode("SELECT", expr);
            }

            else if (line.startsWith("+- 'UnresolvedRelation")) {
                int start = line.indexOf('[');
                int end = line.indexOf(']');
                String table = "";
                if (start != -1 && end != -1 && end > start) {
                    table = line.substring(start + 1, end)
                            .trim();
                }

                SparkPlanNode child = new SparkPlanNode("FROM", table);
                if (root != null) root.addChild(child);
            }
        }

        return root;
    }
}
