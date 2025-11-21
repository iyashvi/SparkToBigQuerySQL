package com.capstone.transformer;

import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanVisitor;
import static com.capstone.constants.Constants.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class SelectConverter extends PlanVisitor {

    private final StringBuilder queryBuilder = new StringBuilder();
    private String selectExpr = "";
    private String fromExpr = "";
    private String whereExpr = "";
    private String groupExpr = "";
    private String havingExpr = "";
    private String orderExpr = "";
    private String limitExpr = "";
    private String offsetExpr = "";
    private String joinTable1 = "";
    private String joinType = "";
    private String joinTable2 = "";
    private String joinAlias1 = "";
    private String joinAlias2 = "";
    private String joinOn = "";
    private String explodeColumn = "";
    private String explodeAlias = "";

    @Override
    public void visit(SparkPlanNode node) {
        if (node == null) return;

        System.out.println(node);

        switch (node.getNodeType()) {
            case SELECT:
                selectExpr = node.getExpression();
                break;
            case FROM:
                fromExpr = node.getExpression();
                break;
            case LATERAL_VIEW:
                explodeColumn = node.getExpression();
                explodeAlias  = node.getAlias1();
                break;
            case JOIN:
                joinTable1 = node.getTable1();
                joinAlias1 = node.getAlias1();
                joinTable2 = node.getTable2();
                joinAlias2 = node.getAlias2();
                joinType   = node.getJoinType();
                joinOn     = node.getJoinCondition();

                JoinInfo j = new JoinInfo();
                j.table = joinTable2;
                j.alias = joinAlias2;
                j.joinType = joinType;
                j.joinCondition = joinOn;
                joinList.add(j);
                break;
            case WHERE:
                whereExpr = node.getExpression();
                break;
            case GROUP_BY:
                groupExpr = node.getExpression();
                break;
            case HAVING:
                havingExpr = node.getExpression();
                break;
            case ORDER_BY:
                orderExpr = node.getExpression();
                break;
            case LIMIT:
                limitExpr = node.getExpression();
                break;
            case OFFSET:
                offsetExpr = node.getExpression();
                break;
        }
    }

    public String getQuery() {
        queryBuilder.setLength(0);

        // SELECT
        if (!selectExpr.isEmpty()) {
            queryBuilder.append(SELECT + SPACE)
                    .append(transformSelectExpr(selectExpr));
            System.out.println("SELECT: " + queryBuilder.toString());
        }
        else if (!fromExpr.isEmpty()) queryBuilder.append(SELECT + "*");

        // JOIN
        if (!joinTable1.isEmpty()) {
            queryBuilder.append(" ").append(FROM).append(" ")
                    .append(joinTable1).append(safeAlias(joinAlias1));

            if (!joinTable2.isEmpty()) {
                queryBuilder.append(" ")
                        .append(joinType).append(" ")
                        .append(joinTable2).append(safeAlias(joinAlias2))
                        .append(" ").append(ON).append(" ").append(joinOn);
            }
        }
        // FROM
        else if (!fromExpr.isEmpty()) {
            queryBuilder.append(SPACE + FROM + SPACE).append(fromExpr);
        }

        if (!joinList.isEmpty()) {

            int start = joinTable2.isEmpty() ? 0 : 1;  // FIX duplicate JOIN

            for (int i = start; i < joinList.size(); i++) {
                JoinInfo ji = joinList.get(i);

                String cleanOn = ji.joinCondition;
                if (cleanOn == null) cleanOn = "";

                cleanOn = cleanOn
                        .replace("]", ")")
                        .replace("[", "(")
                        .replace(":-.", "")
                        .replace(":-", "")
                        .replaceAll("#\\d+", "")
                        .replaceAll("=\\s*:", "=")
                        .trim();

                queryBuilder.append(" ")
                        .append(ji.joinType).append(" ")
                        .append(ji.table).append(safeAlias(ji.alias))
                        .append(" ").append(ON).append(" ").append(cleanOn);
            }
        }


        // EXPLODE -> UNNEST
        if (!explodeColumn.isEmpty()) {
            queryBuilder.append(", UNNEST(")
                    .append(explodeColumn)
                    .append(")");
            if (!explodeAlias.isEmpty()) {
                queryBuilder.append(ALIAS).append(explodeAlias);
            }
        }

        // WHERE
        if (!whereExpr.isEmpty()) {
            queryBuilder.append(SPACE + WHERE + SPACE).append(handleSqlFunctionsAndExpression(whereExpr));
        }

        // GROUP BY
        if (!groupExpr.isEmpty()) {
            queryBuilder.append(SPACE + GROUP_BY + SPACE).append(groupExpr);
        }

        // HAVING
        if (!havingExpr.isEmpty()) {
            queryBuilder.append(SPACE + HAVING + SPACE).append(havingExpr);
        }

        // ORDER BY
        if (!orderExpr.isEmpty()) {
            queryBuilder.append(SPACE + ORDER_BY + SPACE).append(orderExpr);
        }

        // LIMIT
        if (!limitExpr.isEmpty()) {
            queryBuilder.append(SPACE + LIMIT + SPACE).append(limitExpr);
        }

        // OFFSET
        if (!offsetExpr.isEmpty()) {
            queryBuilder.append(SPACE + OFFSET + SPACE).append(offsetExpr);
        }

        return queryBuilder.append(SEMI_COLON).toString().trim();
    }

    private String safeAlias(String alias) {
        if (alias == null) return "";

        alias = alias.trim();

        // FIX 1: Remove all garbage aliases like ":-", "<>", "$anon"
        if (alias.equals(":-") || alias.equals("<>") || alias.matches(".*anon.*")) {
            return "";
        }

        // FIX 2: Remove Spark suffixes like col#12
        alias = alias.replaceAll("#\\d+", "").trim();

        // FIX 3: If alias is empty after cleanup → no alias
        if (alias.isEmpty()) return "";

        // FIX 4: If alias already starts with AS → return it
        if (alias.toUpperCase().startsWith("AS ")) {
            return " " + alias;
        }

        // FIX 5: Normal alias
        return " AS " + alias;
    }


    private String transformSelectExpr(String expr) {

        expr = expr.replaceAll(",\\s*\\)", RIGHT_ROUND_BRACKET);

        List<String> parts = splitTopLevel(expr);

        List<String> cleaned = new ArrayList<>();

        for (String p : parts) {
            String part = p.trim();

            if (part.contains("windowspecdefinition")) {
                part = transformWindowFunction(part);
            }

            part = handleSqlFunctionsAndExpression(part);

            part = part.replaceAll("(?i)( AS \\w+?)(?:#\\d+|\\d+)\\b", "$1")
                    .replaceAll("#\\d+", "");

            cleaned.add(part);
        }

        return String.join(COMMA + SPACE, cleaned);
    }


    private List<String> splitTopLevel(String expr) {

        expr = expr.replaceAll(",\\s*\\)", RIGHT_ROUND_BRACKET);
        List<String> result = new ArrayList<>();

        int depth = 0;
        StringBuilder token = new StringBuilder();

        for (char c : expr.toCharArray()) {
            if (c == '(') depth++;
            if (c == ')') depth--;

            if (c == ',' && depth == 0) {
                result.add(token.toString());
                token.setLength(0);
            } else {
                token.append(c);
            }
        }
        if (!token.isEmpty()) result.add(token.toString());

        return result;
    }


    private String handleSqlFunctionsAndExpression(String expr) {
        if (expr == null || expr.isBlank()) return expr;

        // Where conditions
        expr = expr.replaceAll("=\\s*(?!')(?!\\d+\\b)([^'=\\s][^,;\\)]*)", "= '$1'");
        expr = expr.replaceAll("LIKE\\s*(%?)([^%]+)(%?)", "LIKE '$1$2$3'");

        // DATE functions (convert to BigQuery's format)
        expr = expr.replaceAll("(?<=\\s)(\\d{4}-\\d{2}-\\d{2})(?=\\s*;|\\s*\\b|$)", "'$1'");
        expr = expr.replaceAll("(?i)to_date\\(([^)]+)\\)", "DATE($1)");
        expr = expr.replaceAll("(?i)date_add\\(([^,]+),\\s*(\\d+)\\)", "DATE_ADD($1, INTERVAL $2 DAY)");
        expr = expr.replaceAll("(?i)date_sub\\(([^,]+),\\s*(\\d+)\\)", "DATE_SUB($1, INTERVAL $2 DAY)");
        expr = expr.replaceAll("(?i)datediff\\(([^,]+),\\s*([^)]+)\\)", "DATE_DIFF($1, $2, DAY)");

        // NOW() and similar functions
        expr = expr.replaceAll("(?i)now\\(\\)", "CURRENT_TIMESTAMP()");
        expr = expr.replaceAll("(?i)current_date\\(\\)", "CURRENT_DATE()");

        // UPPER(), LOWER()
        expr = expr.replaceAll("(?i)upper\\(([^)]+)\\)", "UPPER($1)");
        expr = expr.replaceAll("(?i)lower\\(([^)]+)\\)", "LOWER($1)");

        // CASE expressions
        expr = expr.replaceAll("(?i)THEN\\s*(?!')(?!\\d+\\b)([\\w\\-\\s]+?)(?=\\s*(ELSE|WHEN|END))", "THEN '$1'");
        expr = expr.replaceAll("(?i)ELSE\\s*(?!')(?!\\d+\\b)([\\w\\-\\s]+?)(?=\\s*(END))", "ELSE '$1'");

        if (expr.contains("cast")) {
            expr = handleCastExpression(expr);
        }

        expr = expr.replaceAll("(?i)struct\\(([^)]+)\\)", "STRUCT($1)"); // Standard CAST

        // NVL expressions (IFNULL)
        expr = expr.replaceAll("(?i)nvl\\(([^,]+),\\s*([^\\)]+)\\)", "IFNULL($1, $2)"); // NVL to IFNULL


        // MAP_KEYS(expr)
        expr = expr.replaceAll(
                "(?i)map_keys\\(([^)]+)\\)",
                "(SELECT ARRAY_AGG(elem.key) FROM UNNEST($1) AS elem)"
        );

        // MAP_VALUES(expr)
        expr = expr.replaceAll(
                "(?i)map_values\\(([^)]+)\\)",
                "(SELECT ARRAY_AGG(CAST(elem.value AS STRING)) FROM UNNEST($1) AS elem)"
        );

        expr = expr.replaceAll("(?i)array\\s*\\(", "[");
        expr = expr.replaceAll("\\)$", "]");


        // arithmetic operations (e.g., Age + 10)
        expr = expr.replaceAll("(?i)([a-zA-Z_]\\w*)\\s*(\\+|-|\\*|\\/|%)\\s*(\\d+)", "$1 $2 $3");

        return expr;
    }

    private String handleCastExpression(String expr){

        expr = expr.replaceAll("(?i)cast\\s*\\(\\s*([^\\s]+)\\s+as\\s+([^\\s]+)\\s*\\)", "CAST($1 AS $2)");
        expr = expr.replaceAll("(?i)decimal\\s*\\(\\s*([^,]+)\\s*,\\s*([^\\)]+)\\s*\\)", "NUMERIC");

        String regex = "(CAST\\([^)]*?AS\\s+)([a-zA-Z0-9_]+)";

        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(expr);

        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String prefix = matcher.group(1); // e.g., "CAST(o.TotalAmount AS "
            String typeName = matcher.group(2); // e.g., "date"

            String replacement = prefix + typeName.toUpperCase();
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);

        return result.toString();
    }

    private String transformWindowFunction(String expr) {

        String funcName = expr.substring(0, expr.indexOf('(')).toUpperCase().trim();
        String restOfExpression = expr.substring(expr.indexOf('('), expr.indexOf("windowspecdefinition")).trim();
        String func = funcName + restOfExpression;

        int start = expr.indexOf("windowspecdefinition(") + "windowspecdefinition(".length();
        int end = expr.lastIndexOf(")");
        String spec = expr.substring(start, end).trim();

        spec = spec.replace("unspecifiedframe$()", "")
                .replaceAll(",\\s*,", COMMA)
                .replaceAll(",\\s*$", "");

        // Splitting window spec into PARTITION BY and ORDER BY and FRAME parts
        List<String> args = splitTopLevel(spec);

        String partition = "";
        String order = "";
        String frame = "";

        if (!args.isEmpty()) {
            partition = args.get(0).trim(); // first argument is ALWAYS PARTITION column
        }
        if (args.size() > 1) {
            StringBuilder o = new StringBuilder();
            for (int i = 1; i < args.size() && !args.get(i).contains("specifiedwindowframe"); i++) {
                if (!o.isEmpty()) o.append(COMMA + SPACE);
                o.append(args.get(i).trim());
            }
            order = o.toString();
        }

        if (spec.contains("RowFrame") || spec.contains("RangeFrame")) {

            int frameStartIndex = spec.indexOf("specifiedwindowframe(")+ "specifiedwindowframe(".length();
            int frameEndIndex = spec.lastIndexOf(")");
            String cleanFrameSpec = "";

            if (frameStartIndex != -1 && frameEndIndex != -1) {
                cleanFrameSpec = spec.substring(frameStartIndex, frameEndIndex).trim();
                cleanFrameSpec = cleanFrameSpec.replaceAll("\\$\\(\\)", ""); // Remove "$()" if it's present
            }
            // ROWS BETWEEN <start_boundary> AND <end_boundary>
            // RowFrame, unboundedpreceding, currentrow
            // RowFrame, -2, currentrow

            String frameWord = "";
            String startBoundry = "";
            String endBoundry = "";

            List<String> elements = splitTopLevel(cleanFrameSpec);

            if (!elements.isEmpty()) {
                String firstPart = elements.get(0).trim();
                if(firstPart.equals("RangeFrame")) frameWord = "RANGE";
                if(firstPart.equals("RowFrame")) frameWord = "ROWS";

                String secondPart = elements.get(1).trim();
                String thirdPart = elements.get(2).trim();

                if(secondPart.contains("-")){ // PRECEDING
                    startBoundry = secondPart.replace("-", "") + " PRECEDING";
                }
                else if(secondPart.contains("unboundedpreceding")){
                    startBoundry = "UNBOUNDED PRECEDING";
                }
                else startBoundry = secondPart + " FOLLOWING";

                if(thirdPart.contains("-")){ // PRECEDING
                    endBoundry = thirdPart.replace("-", "") + " PRECEDING";
                }
                else if(thirdPart.contains("currentrow")){
                    endBoundry = "CURRENT ROW";
                }
                else if(thirdPart.contains("unboundedfollowing")){
                    endBoundry = "UNBOUNDED FOLLOWING";
                }
                else endBoundry = thirdPart + " FOLLOWING";
            }

            frame = frameWord + " BETWEEN " + startBoundry + " AND " + endBoundry;
        }

        StringBuilder over = new StringBuilder(OVER + LEFT_ROUND_BRACKET);

        if (!partition.isEmpty()) {
            over.append(PARTITION_BY).append(partition);
        }
        if (!order.isEmpty()) {
            if (!partition.isEmpty()) over.append(SPACE);
            over.append(ORDER_BY+SPACE).append(order);
        }
        if (!frame.isEmpty()) {
            if (!order.isEmpty() || !partition.isEmpty()) over.append(SPACE);
            over.append(frame);
        }

        over.append(RIGHT_ROUND_BRACKET);

        String alias = "";
        Matcher m = Pattern.compile("(?i)AS\\s+(\\w+)").matcher(expr);
        if (m.find()) alias = ALIAS + m.group(1);
        return func + SPACE + over + alias;

    }

    private final List<JoinInfo> joinList = new ArrayList<>();

    class JoinInfo {
        String table;
        String alias;
        String joinType;
        String joinCondition;
    }

}