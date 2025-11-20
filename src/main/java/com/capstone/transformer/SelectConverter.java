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
        }

    }

    public String getQuery() {
        queryBuilder.setLength(0);

        // SELECT
        if (!selectExpr.isEmpty()) {
            queryBuilder.append(SELECT + SPACE)
                    .append(transformSelectExpr(selectExpr));
        }
        else if (!fromExpr.isEmpty()) queryBuilder.append(SELECT + "*");


        // JOIN
        if (!joinTable1.isEmpty()) {
            queryBuilder.append(SPACE).append(FROM).append(SPACE)
                    .append(joinTable1).append(safeAlias(joinAlias1));

            if (!joinTable2.isEmpty()) {
                queryBuilder.append(SPACE)
                        .append(joinType).append(SPACE)
                        .append(joinTable2).append(safeAlias(joinAlias2))
                        .append(SPACE).append(ON).append(SPACE).append(joinOn);

            }
        }
        else if (!fromExpr.isEmpty()) {
            queryBuilder.append(SPACE).append(FROM).append(SPACE).append(fromExpr);
        }

        // Explode -> unnest
        if (!explodeColumn.isEmpty()) {
            String baseAlias = "";
            if (fromExpr.contains(" AS ")) {
                baseAlias = fromExpr.split(" AS ")[1].trim();
            } else {
                baseAlias = fromExpr.trim();
            }
            queryBuilder.append(", UNNEST(")
                    .append(baseAlias)
                    .append(".")
                    .append(explodeColumn)
                    .append(")");
            if (!explodeAlias.isEmpty()) {
                queryBuilder.append(" AS ").append(explodeAlias);
            }
        }
        // WHERE
        if (!whereExpr.isEmpty()) {
            whereExpr = fixWhereLiterals(whereExpr);
            queryBuilder.append(SPACE + WHERE + SPACE).append(whereExpr);
        }

        // GROUP BY
        if (!groupExpr.isEmpty()) {
            queryBuilder.append(SPACE + GROUP_BY + SPACE).append(groupExpr);
        }

        // HAVING
        if (!havingExpr.isEmpty()) {
            havingExpr = fixWhereLiterals(havingExpr);
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

        return queryBuilder.append(SEMI_COLON).toString().trim();
    }

    private String safeAlias(String alias) {
        if (alias == null || alias.isBlank()) return "";

        alias = alias.replaceAll("#\\d+", "").trim();

        Matcher m = Pattern.compile("(?i)(.*?)(AS\\s+\\w+)$").matcher(alias);
        if (m.find()) {
            return " " + m.group(2).trim();
        }

        if (alias.toUpperCase().startsWith("AS ")) {
            return " " + alias;
        }

        return " AS " + alias;
    }

    private String transformSelectExpr(String expr) {

        expr = expr.replaceAll(",\\s*\\)", ")")
                .replaceAll("\\)\\)+", ")");

        List<String> parts = splitTopLevel(expr);

        List<String> cleaned = new ArrayList<>();

        for (String p : parts) {
            String part = p.trim();

            if (part.contains("windowspecdefinition")) {
                part = transformWindowFunction(part);
            }
            if (part.toLowerCase().startsWith("map_keys(")) {
                String col = part.substring("map_keys(".length(), part.length() - 1).trim();
                part = "ARRAY(SELECT elem.key FROM UNNEST(" + col + ") AS elem) AS keys";
            }

            part = part.replaceAll("(?i)( AS \\w+?)(?:#\\d+|\\d+)\\b", "$1");

            part = part.replaceAll("#\\d+", "");

            part = ensureAlias(part);


            cleaned.add(part);


        }

        return String.join(COMMA + SPACE, cleaned);
    }

    private List<String> splitTopLevel(String expr) {

        List<String> result = new ArrayList<>();

        int depth = 0;
        StringBuilder token = new StringBuilder();

        for (char c : expr.toCharArray()) {
            if (c == '(') depth++;
            if (c == ')') depth--;

            if (c == ',' && depth == 0) {
                result.add(token.toString());
                System.out.println(token);
                token.setLength(0);
            } else {
                token.append(c);
            }
        }
        if (!token.isEmpty()) result.add(token.toString());
        System.out.println("Result: "+result);

        return result;
    }

    private String transformWindowFunction(String expr) {

        String func = expr.substring(0, expr.indexOf("windowspecdefinition")).trim();

        int start = expr.indexOf("windowspecdefinition(") + "windowspecdefinition(".length();
        int end = expr.lastIndexOf(")");
        String spec = expr.substring(start, end).trim();

        spec = spec.replace("unspecifiedframe$()", "")
                .replaceAll(",\\s*,", ",")
                .replaceAll(",\\s*$", "");

        // Splitting window spec into PARTITION BY and ORDER BY parts
        List<String> args = splitTopLevel(spec);

        String partition = "";
        String order = "";

        if (!args.isEmpty()) {
            partition = args.get(0).trim(); // first argument is ALWAYS PARTITION column
        }
        if (args.size() > 1) {
            StringBuilder o = new StringBuilder();
            for (int i = 1; i < args.size(); i++) {
                if (o.length() > 0) o.append(", ");
                o.append(args.get(i).trim());
            }
            order = o.toString();
        }

        StringBuilder over = new StringBuilder("OVER (");

        if (!partition.isEmpty()) {
            over.append("PARTITION BY ").append(partition);
        }
        if (!order.isEmpty()) {
            if (!partition.isEmpty()) over.append(" ");
            over.append("ORDER BY ").append(order);
        }

        over.append(")");

        String alias = "";
        Matcher m = Pattern.compile("(?i)AS\\s+(\\w+)").matcher(expr);
        if (m.find()) alias = " AS " + m.group(1);
        return func + " " + over + alias;

    }
    private String fixWhereLiterals(String expr) {
        if (expr == null || expr.isBlank()) return expr;

        expr = expr.replaceAll("(=)\\s*([A-Za-z_][A-Za-z0-9_]*(?:\\s+[A-Za-z0-9_]+)+)", "$1 '$2'");

        expr = expr.replaceAll("(=)\\s*(?!')(?!\\d+\\b)([A-Za-z_][A-Za-z0-9_]*)", "$1 '$2'");

        return expr;
    }

    private String ensureAlias(String expr) {
        return expr;
    }

    public void reset() {
        this.queryBuilder.setLength(0);
        this.selectExpr = "";
        this.fromExpr = "";
        this.whereExpr = "";
        this.groupExpr = "";
        this.havingExpr = "";
        this.orderExpr = "";
        this.limitExpr = "";
        this.joinType = "";
        this.joinOn = "";
        this.joinTable1 = "";
        this.joinTable2 = "";
        this.joinAlias1 = "";
        this.joinAlias2 = "";
    }

}

