package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString) {
        String[] lines = planString.split("\n");
        List<SparkPlanNode> nodes = new ArrayList<>();
        boolean fromAdded = false;   // prevent duplicate FROM
        SparkPlanNode lastJoin = null; // track most recent JOIN
        String pendingAlias = null;



        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            SparkPlanNode node = null;

            // SELECT
            if ((line.contains("Project")) &&
                    nodes.stream().noneMatch(n -> n.getNodeType().equals("SELECT"))) {
                //String expr = extractValue(line).replace("'", "").trim();
                node = new SparkPlanNode("SELECT", extractValue(line));
            }


            // FROM
            else if (line.startsWith(":- 'SubqueryAlias") || line.startsWith("+- 'SubqueryAlias")) {
                pendingAlias = line.split(" ")[2].trim(); // store alias e.g., e or d
            }
            else if (line.contains("'UnresolvedRelation")) {
                String table = extractValue(line).trim();

                if (lastJoin != null) {
                    // This table belongs to the JOIN
                    if (!lastJoin.hasTable1()) lastJoin.setTable1(table, pendingAlias);
                    else lastJoin.setTable2(table, pendingAlias);
                } else {
                    // No join: normal FROM
                    if (!fromAdded) {
                        nodes.add(new SparkPlanNode("FROM", table + (pendingAlias != null ? " AS " + pendingAlias : "")));
                        fromAdded = true;
                    }
                }

                pendingAlias = null;
            }
            else if (line.contains("Join")) {
                String joinType = extractJoinType(line);
                String condition = extractJoinCondition(line);

                SparkPlanNode joinNode = new SparkPlanNode("JOIN", "");
                joinNode.setJoinType(joinType);
                joinNode.setJoinCondition(condition);

                lastJoin = joinNode;
                nodes.add(joinNode);
            }

            // WHERE
            else if (line.startsWith("+- 'Filter")) {
                node = new SparkPlanNode("WHERE", extractFilterCondition(line));
            }


            else if (line.startsWith("'Aggregate") || line.startsWith("Aggregate")) {
                String groupExpr = extractGroupByColumns(line);
                String aggExpr = extractAggregateFunctions(line);

                SparkPlanNode groupNode = new SparkPlanNode("GROUP BY", groupExpr);
                SparkPlanNode selectNode = new SparkPlanNode("SELECT", aggExpr);

                nodes.add(selectNode);
                nodes.add(groupNode);


            }

            else if (line.startsWith("'UnresolvedHaving")) {
                String havingCondition = extractHavingCondition(line);
                SparkPlanNode havingNode = new SparkPlanNode("HAVING", havingCondition);
                nodes.add(havingNode);
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

        // Correct SQL order (SELECT → FROM → JOIN ->  WHERE → ORDER → LIMIT)
        SparkPlanNode root = null;
        SparkPlanNode last = null;

        for (String type : List.of( "SELECT", "FROM", "JOIN", "WHERE" , "GROUP BY", "HAVING", "ORDER BY", "LIMIT")) {
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
    private String extractJoinType(String line) {
        String lower = line.toLowerCase();

        if (lower.contains("left")) return "LEFT JOIN";
        if (lower.contains("right")) return "RIGHT JOIN";
        if (lower.contains("full")) return "FULL JOIN";
        if (lower.contains("cross")) return "CROSS JOIN";
        return "INNER JOIN";
    }

    private String extractJoinCondition(String line) {
        int s = line.indexOf('(');
        int e = line.lastIndexOf(')');
        if (s != -1 && e != -1) {
            return line.substring(s + 1, e)
                    .replaceAll("#\\d+", "")
                    .replace("'", "")
                    .trim();
        }
        return "";
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