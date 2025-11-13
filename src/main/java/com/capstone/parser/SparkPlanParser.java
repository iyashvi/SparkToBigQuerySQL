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

            // GROUP BY (Aggregate node)
            else if (line.startsWith("'Aggregate") || line.startsWith("Aggregate")) {
                String groupExpr = extractGroupByColumns(line);
                String aggExpr = extractAggregateFunctions(line);

                // Two nodes: GROUP BY and SELECT (aggregations)
                SparkPlanNode groupNode = new SparkPlanNode("GROUP BY", groupExpr);
                SparkPlanNode selectNode = new SparkPlanNode("SELECT", aggExpr);
                nodes.add(selectNode);
                nodes.add(groupNode);
                continue;
            }

            // ORDER BY
            else if (line.contains("Sort"))  {
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

        // Find SELECT node first
        for (SparkPlanNode n : nodes) {
            if (n.getNodeType().equals("SELECT")) {
                root = n;
                last = n;
                break;
            }
        }

        for (String type : List.of( "FROM", "WHERE" , "GROUP BY", "ORDER BY", "LIMIT")) {
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
            return line.substring(start + 1, end)
                    .replace("'", "")
                    .replace("#", "")
                    .trim();
        }
        return "unknown_condition";
    }

    private String extractGroupByColumns(String line) {
        int firstStart = line.indexOf('[');
        int firstEnd = line.indexOf(']');
        if (firstStart != -1 && firstEnd != -1) {
            return line.substring(firstStart + 1, firstEnd)
                    .replace("'", "")
                    .replace("#", "")
                    .trim();
        }
        return "unknown_group_columns";
    }
    private String extractAggregateFunctions(String line) {
        int firstEnd = line.indexOf(']');
        int secondStart = line.indexOf('[', firstEnd + 1);
        int secondEnd = line.indexOf(']', secondStart + 1);

        if (secondStart != -1 && secondEnd != -1) {
            String aggPart = line.substring(secondStart + 1, secondEnd)
                    .replace("unresolvedalias(", "")
                    .replace("None", "")
                    .replace("'", "")
                    .replace(")", "")
                    .replace("#", "")
                    .trim();


            aggPart = aggPart.replaceAll("\\s*,\\s*", ", ");
            aggPart = aggPart.replaceAll(",\\s*$", "");
            aggPart = aggPart.replaceAll("\\s+", " ");

            return aggPart;
        }

        return "unknown_aggregate";
    }


}