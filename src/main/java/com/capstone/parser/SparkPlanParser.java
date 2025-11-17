package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import static com.capstone.constants.Constants.*;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString) {
        String[] lines = planString.split(NEW_LINE);
        List<SparkPlanNode> nodes = new ArrayList<>();
        boolean fromAdded = false;   // prevent duplicate FROM
        SparkPlanNode lastJoin = null; // track most recent JOIN

        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            SparkPlanNode node = null;

            // SELECT
            if (line.contains("Project") && nodes.stream().noneMatch(n -> n.getNodeType().equals(SELECT))) {
                node = new SparkPlanNode(SELECT, extractValue(line));
            }

            // FROM
            else if (line.contains("UnresolvedRelation")) {
                String tableName = extractValue(line).trim();

                if (lastJoin != null) {
                    // Fill in TABLE1 or TABLE2 placeholders
                    String expr = lastJoin.getExpression();
                    if (expr.contains("TABLE1=?") && line.contains(":-")) {
                        expr = expr.replace("TABLE1=?", "TABLE1=" + tableName);
                    } else if (expr.contains("TABLE2=?") && line.contains("+-")) {
                        expr = expr.replace("TABLE2=?", "TABLE2=" + tableName);
                    }
                    lastJoin.setExpression(expr);
                } else if (!fromAdded) {
                    node = new SparkPlanNode(FROM, tableName);
                    fromAdded = true;
                }
            }

            //JOIN
            else if (line.contains("Join")) {
                // Create JOIN node with placeholders for table names
                node = new SparkPlanNode(JOIN, "TABLE1=?, TABLE2=? " + extractJoinDetails(line));
                lastJoin = node;
            }

            // WHERE
            else if (line.contains("Filter")) {
                node = new SparkPlanNode(WHERE, extractFilterCondition(line));
            }

            // GROUP BY (Aggregate node)
            else if (line.contains("Aggregate")) {

                String groupExpr = extractValue(line);
                String selectExpr = extractFullSelectList(line);
                SparkPlanNode selectNode = new SparkPlanNode(SELECT, selectExpr);
                SparkPlanNode groupNode = new SparkPlanNode(GROUP_BY, groupExpr);

                nodes.add(selectNode);
                nodes.add(groupNode);
                continue;
            }

            // HAVING
            else if (line.contains("UnresolvedHaving")) {
                String havingCondition = extractFilterCondition(line);
                node = new SparkPlanNode(HAVING, havingCondition);
            }


            // ORDER BY
            else if (line.contains("Sort"))  {
                node = new SparkPlanNode(ORDER_BY, extractValue(line));
            }

            // LIMIT
            else if (line.startsWith("'GlobalLimit") || line.startsWith("'LocalLimit")) {
                node = new SparkPlanNode(LIMIT, extractValue(line));
            }

            if (node != null) {
                System.out.println("=====node: " + node);  // -- For Debugging
                nodes.add(node);
            }
        }

        // Correct SQL order (SELECT → FROM → JOIN ->  WHERE → ORDER → LIMIT)
        SparkPlanNode root = null;
        SparkPlanNode last = null;

        for (String type : List.of( SELECT, FROM, JOIN,  WHERE , GROUP_BY, HAVING, ORDER_BY, LIMIT)) {
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

    private String replacement(String line, int start, int end) {
        return line.substring(start + 1, end)
                .replace(SINGLE_INVERTED_COMMA, "")
                .replace(HASHTAG, "")
                .replace("unresolvedalias(", "")
                .replace("unspecifiedframe$(", "")
                .replace("None", "")
                .replaceAll("\\s+", SPACE)
                .replaceAll("#\\d+", "")
                .trim();
    }

    private String extractValue(String line) {
        int start = line.indexOf(LEFT_SQUARE_BRACKET);
        int end = line.indexOf(RIGHT_SQUARE_BRACKET);
        if (start != -1 && end != -1 && end > start) {
            return replacement(line, start, end);
        }
        String[] parts = line.split(SPACE);
        return parts.length > 1 ? parts[1].trim() : "unknown";
    }

    private String extractFilterCondition(String line) {
        int start = line.indexOf(LEFT_ROUND_BRACKET);
        int end = line.lastIndexOf(RIGHT_ROUND_BRACKET);
        if (start != -1 && end != -1 && end > start) {
            replacement(line, start, end);
        }
        return "unknown_condition";
    }

    private String extractFullSelectList(String line) {
        int firstEnd = line.indexOf(RIGHT_SQUARE_BRACKET);
        int secondStart = line.indexOf(LEFT_SQUARE_BRACKET, firstEnd + 1);
        int secondEnd = line.indexOf(RIGHT_SQUARE_BRACKET, secondStart + 1);

        if (secondStart == -1 || secondEnd == -1) return "";

        String raw = replacement(line, secondStart, secondEnd);

        // Split by comma but preserve window functions
        List<String> parts = new ArrayList<>();
        int depth = 0;
        StringBuilder buf = new StringBuilder();
        for (char c : raw.toCharArray()) {
            if (c == '(') depth++;
            if (c == ')') depth--;
            if (c == ',' && depth == 0) {
                parts.add(buf.toString().trim());
                buf.setLength(0);
            } else {
                buf.append(c);
            }
        }
        if (buf.length() > 0) parts.add(buf.toString().trim());

        // Clean each expression
        List<String> cleaned = new ArrayList<>();
        for (String p : parts) {
            if (p.isEmpty()) continue;
            cleaned.add(p.trim());
        }

        return String.join(", ", cleaned);
    }

    private String extractJoinDetails(String line) {
        String joinType = "INNER";
        String condition = "";

        if (line.toLowerCase().contains("left")) joinType = "LEFT OUTER";
        else if (line.toLowerCase().contains("right")) joinType = "RIGHT OUTER";
        else if (line.toLowerCase().contains("full")) joinType = "FULL OUTER";
        else if (line.toLowerCase().contains("cross")) joinType = "CROSS";

        if (line.contains(LEFT_ROUND_BRACKET) && line.contains(RIGHT_ROUND_BRACKET)) {
            int start = line.indexOf(LEFT_ROUND_BRACKET);
            int end = line.lastIndexOf(RIGHT_ROUND_BRACKET);
            if (start >= 0 && end > start) {
                condition = replacement(line, start, end);
            }
        }
        return joinType + " ON " + condition;
    }
}