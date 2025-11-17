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
        String pendingAlias = null;


        for (String line : lines) {
            line = line.trim();
            System.out.println("==== PARSER DEBUG LINE: " + line);

            SparkPlanNode node = null;

            // SELECT
            if ((line.contains("Project")) &&
                    nodes.stream().noneMatch(n -> n.getNodeType().equals("SELECT"))) {
                String expr = extractValue(line).replace("'", "").trim();
                expr = cleanAlias(expr);
                node = new SparkPlanNode("SELECT", expr);
            }


            // FROM
            else if (line.startsWith(":- 'SubqueryAlias") || line.startsWith("+- 'SubqueryAlias")) {
                pendingAlias = line.split(" ")[2].trim();
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
            else if (line.contains("Filter")) {
                node = new SparkPlanNode(WHERE, extractFilterCondition(line));
            }

//            // GROUP BY (Aggregate node)
            else if (line.contains("Aggregate")) {
                String groupExpr = extractGroupByColumns(line);
                String aggExpr = extractAggregateFunctions(line);

                // Two nodes: GROUP BY and SELECT (aggregations)
                SparkPlanNode groupNode = new SparkPlanNode(GROUP_BY, groupExpr);
                SparkPlanNode selectNode = new SparkPlanNode(SELECT, aggExpr);
                nodes.add(selectNode);
                nodes.add(groupNode);

                continue;

            }
            else if (line.contains("UnresolvedHaving")) {
                String havingCondition = extractHavingCondition(line);
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

    private String extractValue(String line) {
        int start = line.indexOf(LEFT_SQUARE_BRACKET);
        int end = line.indexOf(RIGHT_SQUARE_BRACKET);
        if (start != -1 && end != -1 && end > start) {
            return line.substring(start + 1, end).replace(SINGLE_INVERTED_COMMA, "").trim();
        }
        String[] parts = line.split(SPACE);
        return parts.length > 1 ? parts[1].trim() : "";
    }

    private String cleanAlias(String expr){
        return expr.replaceAll("#\\d+","").trim();
    }

    private String cleanExpression(String expr){
        return expr.replaceAll("#\\d+","").trim();
    }

    private String extractFilterCondition(String line) {
        int start = line.indexOf(LEFT_ROUND_BRACKET);
        int end = line.lastIndexOf(RIGHT_ROUND_BRACKET);
        if (start != -1 && end != -1 && end > start) {
            return line.substring(start + 1, end)
                    .replace(SINGLE_INVERTED_COMMA, "")
                    .replace(HASHTAG, "")
                    .trim();
        }
        return "unknown_condition";
    }

    private String extractGroupByColumns(String line) {
        int firstStart = line.indexOf(LEFT_SQUARE_BRACKET);
        int firstEnd = line.indexOf(RIGHT_SQUARE_BRACKET);
        if (firstStart != -1 && firstEnd != -1) {
            return line.substring(firstStart + 1, firstEnd)
                    .replace(SINGLE_INVERTED_COMMA, "")
                    .replace(HASHTAG, "")
                    .trim();
        }
        return "unknown_group_columns";
    }



    private String extractAggregateFunctions(String line) {
        int firstEnd = line.indexOf(RIGHT_SQUARE_BRACKET);
        int secondStart = line.indexOf(LEFT_SQUARE_BRACKET, firstEnd + 1);
        int secondEnd = line.indexOf(RIGHT_SQUARE_BRACKET, secondStart + 1);

        if (secondStart != -1 && secondEnd != -1) {
            String aggPart = line.substring(secondStart + 1, secondEnd)
                    .replace("unresolvedalias(", "")
                    .replace("unspecifiedframe$(", "")
                    .replace("None", "")
                    .replace(SINGLE_INVERTED_COMMA, "")
                    .replace(RIGHT_ROUND_BRACKET, "")
                    .replace(HASHTAG, "")
                    .trim();

            String[] funcs = aggPart.split(COMMA);
            List<String> cleaned = new ArrayList<>();
            for (String f : funcs) {
                f = f.trim();
                if (f.isEmpty()) continue;


                if (!f.contains(LEFT_ROUND_BRACKET)) continue;

                // Format function properly
                String fn = f.substring(0, f.indexOf(LEFT_ROUND_BRACKET)).toUpperCase();
                String arg = f.substring(f.indexOf(LEFT_ROUND_BRACKET) + 1).trim();
                cleaned.add(fn + LEFT_ROUND_BRACKET + arg + RIGHT_ROUND_BRACKET);


                String alias = "";
                if (arg.contains(" AS ")) {
                    String[] parts = arg.split(" AS ");
                    arg = parts[0].trim();
                    alias = parts[1].trim();
                }

                String full = fn + LEFT_ROUND_BRACKET + arg + RIGHT_ROUND_BRACKET;
                if (!alias.isEmpty()) full += " AS " + cleanAlias(alias);

                cleaned.add(full);

            }

            return String.join(COMMA+SPACE, cleaned);
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
        int s = line.indexOf(LEFT_ROUND_BRACKET);
        int e = line.lastIndexOf(RIGHT_ROUND_BRACKET);
        if (s != -1 && e != -1) {
            return line.substring(s + 1, e)
                    .replaceAll("#\\d+", "")
                    .replace(SINGLE_INVERTED_COMMA, "")
                    .trim();
        }
        return "";
    }


    private String extractHavingCondition(String line) {
        int start = line.indexOf(LEFT_ROUND_BRACKET);
        int end = line.lastIndexOf(RIGHT_ROUND_BRACKET);
        if (start != -1 && end != -1 && end > start) {
            return line.substring(start + 1, end)
                    .replace(SINGLE_INVERTED_COMMA, "")
                    .replace(HASHTAG, "")
                    .trim();
        }
        return "";
    }


















}