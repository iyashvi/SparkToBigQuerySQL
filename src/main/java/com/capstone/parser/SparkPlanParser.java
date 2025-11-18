package com.capstone.parser;
import com.capstone.model.SparkPlanNode;
import static com.capstone.constants.Constants.*;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SparkPlanParser {

    public SparkPlanNode parse(String planString)
    {
        String[] lines = planString.split(NEW_LINE);
        List<SparkPlanNode> nodes = new ArrayList<>();
        boolean fromAdded = false;   // prevent duplicate FROM
        SparkPlanNode lastJoin = null; // track most recent JOIN
        String pendingAlias = null;

        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            SparkPlanNode node = null;

            // SELECT
            if ((line.contains("Project")) && nodes.stream().noneMatch(n -> n.getNodeType().equals(SELECT))) {
                node = new SparkPlanNode(SELECT, extractValue(line));
            }

            // ALIAS
            else if (line.contains("SubqueryAlias")) {
                pendingAlias = line.split(SPACE)[2].trim(); // store alias e.g., e or d
            }

            // FROM
            else if (line.contains("'UnresolvedRelation")) {
                String table = extractValue(line).trim();

                if (lastJoin != null) {
                    // This table belongs to the JOIN
                    if (!lastJoin.hasTable1()) lastJoin.setTable1(table, pendingAlias);
                    else lastJoin.setTable2(table, pendingAlias);
                }
                else if (!fromAdded) {
                    nodes.add(new SparkPlanNode(FROM, table + (pendingAlias != null ? " AS " + pendingAlias : "")));

                    fromAdded = true;
                }

                pendingAlias = null;
            }

            //JOIN
            else if (line.contains("Join")) {
                String joinType = extractJoinType(line);
                String joinCondition = extractCondition(line);

                node = new SparkPlanNode("JOIN", "");
                node.setJoinType(joinType);
                node.setJoinCondition(joinCondition);

                lastJoin = node;
            }

            // WHERE
            else if (line.contains("Filter")) {
                node = new SparkPlanNode(WHERE, extractCondition(line));
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
                String havingCondition = extractCondition(line);
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
        return parts.length > 1 ? parts[1].trim() : "unknown_value";
    }

    private String extractCondition(String line) {
        int start = line.indexOf(LEFT_ROUND_BRACKET);
        int end = line.lastIndexOf(RIGHT_ROUND_BRACKET);
        if (start != -1 && end != -1 && end > start) {
            return replacement(line, start, end);
        }
        return "unknown_condition";
    }

    private String extractJoinType(String line) {
        String lower = line.toLowerCase();

        if (lower.contains("left")) return "LEFT JOIN";
        if (lower.contains("right")) return "RIGHT JOIN";
        if (lower.contains("full")) return "FULL JOIN";
        if (lower.contains("cross")) return "CROSS JOIN";
        return "INNER JOIN";
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
        if (!buf.isEmpty()) parts.add(buf.toString().trim());


        List<String> cleaned = new ArrayList<>();
        for (String p : parts) {
            if (p.isEmpty()) continue;
            cleaned.add(p.trim());
        }

        return String.join(", ", cleaned);
    }
}