package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString) {
        String[] lines = planString.split("\n");
        List<SparkPlanNode> nodes = new ArrayList<>();
        boolean fromAdded = false;   // prevent duplicate FROM
        SparkPlanNode lastJoin = null; // track most recent JOIN


        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            SparkPlanNode node = null;

            // SELECT
            if ((line.startsWith("Project") || line.startsWith("'Project")) &&
                    !line.startsWith("+-") && !line.startsWith(":-") &&
                    nodes.stream().noneMatch(n -> n.getNodeType().equals("SELECT"))) {
                String expr = extractValue(line).replace("'", "").trim();
                node = new SparkPlanNode("SELECT", expr);
            }


            // FROM
            else if (line.contains("'UnresolvedRelation")) {
                String tableName = extractValue(line).trim();

                if (lastJoin != null) {
                    // Fill in TABLE1 or TABLE2 placeholders
                    String expr = lastJoin.getExpression();
                    if (expr.contains("TABLE1=?")) {
                        expr = expr.replace("TABLE1=?", "TABLE1=" + tableName);
                    } else if (expr.contains("TABLE2=?")) {
                        expr = expr.replace("TABLE2=?", "TABLE2=" + tableName);
                    }
                    lastJoin.setExpression(expr);
                } else if (!fromAdded) {
                    node = new SparkPlanNode("FROM", tableName);
                    fromAdded = true;
                }
            }

            //JOIN
            else if (line.contains("Join")) {
                // Create JOIN node with placeholders for table names
                node = new SparkPlanNode("JOIN", "TABLE1=?, TABLE2=? " + extractJoinDetails(line));
                lastJoin = node;
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

            else if (line.startsWith("'UnresolvedHaving")) {
                String havingCondition = extractHavingCondition(line);
                node = new SparkPlanNode("HAVING", havingCondition);
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

        // Correct SQL order (SELECT → FROM → JOIN ->  WHERE → ORDER → LIMIT)
        SparkPlanNode root = null;
        SparkPlanNode last = null;

        // Find SELECT node first
//        for (SparkPlanNode n : nodes) {
//            if (n.getNodeType().equals("SELECT")) {
//                root = n;
//                last = n;
//                break;
//            }
//        }

        for (String type : List.of( "SELECT", "FROM", "JOIN",  "WHERE" , "GROUP BY", "HAVING", "ORDER BY", "LIMIT")) {
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

            String[] funcs = aggPart.split(",");
            List<String> cleaned = new ArrayList<>();
            for (String f : funcs) {
                f = f.trim();
                if (f.isEmpty()) continue;

                // Keep only aggregate functions (skip plain column names)
                if (!f.contains("(")) continue;

                // Format function properly
                String fn = f.substring(0, f.indexOf('(')).toUpperCase();
                String arg = f.substring(f.indexOf('(') + 1).trim();
                cleaned.add(fn + "(" + arg + ")");
            }

            return String.join(", ", cleaned);
        }
        return "";
    }


    private String extractJoinDetails(String line) {
        String joinType = "INNER";
        String condition = "";

        if (line.toLowerCase().contains("left")) joinType = "LEFT OUTER";
        else if (line.toLowerCase().contains("right")) joinType = "RIGHT OUTER";
        else if (line.toLowerCase().contains("full")) joinType = "FULL OUTER";
        else if (line.toLowerCase().contains("cross")) joinType = "CROSS";

        if (line.contains("(") && line.contains(")")) {
            int s = line.indexOf('(');
            int e = line.lastIndexOf(')');
            if (s >= 0 && e > s) {
                condition = line.substring(s + 1, e)
                        .replaceAll("#\\d+", "")
                        .replaceAll("'", "")
                        .replaceAll("\\s+", " ")
                        .trim();
            }
        }
        return joinType + " ON " + condition;
    }

    private String extractHavingCondition(String line) {
        int start = line.indexOf('(');
        int end = line.lastIndexOf(')');
        if (start != -1 && end != -1 && end > start) {
            return line.substring(start + 1, end)
                    .replace("'", "")
                    .replace("#", "")
                    .trim();
        }
        return "";
    }
}