package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString) {
        String[] lines = planString.split("\n");
        List<SparkPlanNode> nodes = new ArrayList<>();

        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            SparkPlanNode node = null;

            if (line.contains("Project")) {
                String expr = extractValue(line).replace("'", "").trim();
                node = new SparkPlanNode("SELECT", expr);
            }

            // FROM
            else if (line.startsWith("+- 'UnresolvedRelation")) {
                node = new SparkPlanNode("FROM", extractValue(line));
            }

            // WHERE
            else if (line.startsWith("+- 'Filter")) {
                node = new SparkPlanNode("WHERE", extractFilterCondition(line));
            }

            // ORDER BY
            else if (line.contains("Sort")) {
                node = new SparkPlanNode("ORDER BY", extractValue(line));
            }

            // LIMIT
            else if (line.startsWith("'GlobalLimit") || line.startsWith("'LocalLimit")) {
                node = new SparkPlanNode("LIMIT", extractValue(line));
            }

            if (node != null) {
                System.out.println("=====node: " + node);
                nodes.add(node);
            }
        }

        // Correct SQL order (SELECT → FROM → WHERE → ORDER → LIMIT)
        SparkPlanNode root = null;
        SparkPlanNode last = null;

        for (String type : List.of("SELECT", "FROM", "WHERE", "ORDER BY", "LIMIT")) {
            for (SparkPlanNode n : nodes) {
                if (n.getNodeType().equals(type)) {
                    if (root == null) {
                        root = n;
                        last = root;
                    } else {
                        last.addChild(n);
                        last = n;
                    }
                }
            }
        }

        return root;
    }

    private String extractValue(String line) {
        int start = line.indexOf('[');
        int end = line.indexOf(']');
        if (start != -1 && end != -1 && end > start) {
            return line.substring(start + 1, end).replace("'", "").trim();
        }
        String[] parts = line.split(" ");
        return parts.length > 1 ? parts[1].trim() : "";
    }

    private String extractFilterCondition(String line) {
        int start = line.indexOf('(');
        int end = line.lastIndexOf(')');
        if (start != -1 && end != -1 && end > start) {
            return line.substring(start + 1, end).replace("'", "").trim();
        }
        return "unknown_condition";
    }

}