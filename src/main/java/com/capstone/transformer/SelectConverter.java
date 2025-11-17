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
    private String joinExpr = "";
    private String whereExpr = "";
    private String groupExpr = "";
    private String havingExpr = "";
    private String orderExpr = "";
    private String limitExpr = "";

    @Override
    public void visit(SparkPlanNode node) {
        if (node == null) return;

        System.out.println(node);

        switch (node.getNodeType()) {
            case SELECT:
                selectExpr = node.getExpression();
                System.out.println(selectExpr);
                break;
            case FROM:
                fromExpr = node.getExpression();
                break;
            case JOIN:
                joinExpr = node.getExpression();
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

//    public String getQuery() {
//        queryBuilder.setLength(0);
//
//        if (!selectExpr.isEmpty()) queryBuilder.append(SELECT+SPACE).append(selectExpr);
//        if (!fromExpr.isEmpty()) queryBuilder.append(SPACE+FROM+SPACE).append(fromExpr);
//        if (!joinExpr.isEmpty()) {
//            String table1 = "", table2 = "", joinType = "", onCond = "";
//
//            try {
//                // Example joinExpr:
//                // TABLE1=Employees, TABLE2=Department LEFT OUTER ON Employees.EmployeeID = department.EmployeeID
//                int t1s = joinExpr.indexOf("TABLE1=") + 7;
//                int t2s = joinExpr.indexOf("TABLE2=");
//                int joinTypeStart = joinExpr.indexOf(" ", t2s + 7);
//                int onIdx = joinExpr.indexOf(" ON ");
//
//                table1 = joinExpr.substring(t1s, t2s - 2).trim();  // Employees
//                table2 = joinExpr.substring(t2s + 7, joinTypeStart).trim(); // Department
//                joinType = joinExpr.substring(joinTypeStart, onIdx).trim(); // LEFT OUTER
//                onCond = joinExpr.substring(onIdx + 4).trim(); // condition
//
//            } catch (Exception e) {
//                System.err.println("JOIN parsing error: " + e.getMessage());
//            }
//
//            queryBuilder.append(SPACE+FROM+SPACE)
//                    .append(table1)
//                    .append(SPACE)
//                    .append(joinType)
//                    .append(SPACE+JOIN+SPACE)
//                    .append(table2)
//                    .append(SPACE+ON+SPACE)
//                    .append(onCond);
//        }
//
//
//        if (!whereExpr.isEmpty()) queryBuilder.append(SPACE+WHERE+SPACE).append(whereExpr);
//        if (!groupExpr.isEmpty()) queryBuilder.append(SPACE+GROUP_BY+SPACE).append(groupExpr);
//        if (!havingExpr.isEmpty()) queryBuilder.append(SPACE+HAVING+SPACE).append(havingExpr);
//        if (!orderExpr.isEmpty()) queryBuilder.append(SPACE+ORDER_BY+SPACE).append(orderExpr);
//        if (!limitExpr.isEmpty()) queryBuilder.append(SPACE+LIMIT+SPACE).append(limitExpr);
//
//        return queryBuilder.append(SEMI_COLON).toString().trim();
//    }


    public String getQuery() {
        queryBuilder.setLength(0);

        // ---- SELECT
        if (!selectExpr.isEmpty()) {
            queryBuilder.append(SELECT + SPACE)
                    .append(transformSelectExpr(selectExpr));
        }

        // ---- FROM / JOIN
        if (!joinExpr.isEmpty()) {
            queryBuilder.append(SPACE).append(parseJoinExpr(joinExpr));
        } else if (!fromExpr.isEmpty()) {
            queryBuilder.append(SPACE + FROM + SPACE).append(fromExpr);
        }

        // ---- WHERE
        if (!whereExpr.isEmpty()) {
            queryBuilder.append(SPACE + WHERE + SPACE).append(whereExpr);
        }

        // ---- GROUP BY
        if (!groupExpr.isEmpty()) {
            queryBuilder.append(SPACE + GROUP_BY + SPACE).append(groupExpr);
        }

        // ---- HAVING
        if (!havingExpr.isEmpty()) {
            queryBuilder.append(SPACE + HAVING + SPACE).append(havingExpr);
        }

        // ---- ORDER BY
        if (!orderExpr.isEmpty()) {
            queryBuilder.append(SPACE + ORDER_BY + SPACE).append(orderExpr);
        }

        // ---- LIMIT
        if (!limitExpr.isEmpty()) {
            queryBuilder.append(SPACE + LIMIT + SPACE).append(limitExpr);
        }

        return queryBuilder.append(SEMI_COLON).toString().trim();
    }


    // ----------------------
    // Transform SELECT expression: handle window functions & unresolvedalias
    // ----------------------
    private String transformSelectExpr(String expr) {

        // 1. Remove Spark’s broken trailing artifacts like ", )"
        expr = expr.replaceAll(",\\s*\\)", ")")
                .replaceAll("\\)\\)+", ")");

        // 2. Split the SELECT expression robustly based on commas at top level
        List<String> parts = splitTopLevel(expr);

        List<String> cleaned = new ArrayList<>();

        for (String p : parts) {
            String part = p.trim();

            // Handle window functions
            if (part.contains("windowspecdefinition")) {
                part = transformWindowFunction(part);
            }

            // Remove unresolvedalias wrappers
            part = part.replaceAll("unresolvedalias\\(([^,]+), None\\)", "$1");

            // Fix repeated parentheses
            part = part.replaceAll("\\)+$", ")");

            cleaned.add(part);
        }

        return String.join(", ", cleaned);
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

        // Extract function name: RANK(), DENSE_RANK(), ROW_NUMBER(), SUM(col), etc.
        String func = expr.substring(0, expr.indexOf("windowspecdefinition")).trim();

        // Extract content inside windowspecdefinition(...)
        int start = expr.indexOf("windowspecdefinition(") + "windowspecdefinition(".length();
        int end = expr.lastIndexOf(")");
        String spec = expr.substring(start, end).trim();

        // Cleanup Spark artifacts
        spec = spec.replace("unspecifiedframe$()", "")
                .replaceAll(",\\s*,", ",")
                .replaceAll(",\\s*$", "");

        // Split window spec into PARTITION BY and ORDER BY parts
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

        // Construct final SQL syntax
        StringBuilder over = new StringBuilder("OVER(");

        if (!partition.isEmpty()) {
            over.append("PARTITION BY ").append(partition);
        }
        if (!order.isEmpty()) {
            if (!partition.isEmpty()) over.append(" ");
            over.append("ORDER BY ").append(order);
        }

        over.append(")");

        return func + " " + over;
    }

    // ----------------------
    // Parse JOIN expression
    // ----------------------
    private String parseJoinExpr(String joinExpr) {
        try {
            int t1s = joinExpr.indexOf("TABLE1=") + 7;
            int t2s = joinExpr.indexOf("TABLE2=");
            int joinTypeStart = joinExpr.indexOf(" ", t2s + 7);
            int onIdx = joinExpr.indexOf(" ON ");

            String table1 = joinExpr.substring(t1s, t2s - 2).trim();
            String table2 = joinExpr.substring(t2s + 7, joinTypeStart).trim();
            String joinType = joinExpr.substring(joinTypeStart, onIdx).trim();
            String onCond = joinExpr.substring(onIdx + 4).trim();

            return FROM + SPACE + table1 + SPACE + joinType + SPACE + JOIN + SPACE + table2 + SPACE + ON + SPACE + onCond;
        } catch (Exception e) {
            System.err.println("JOIN parsing error: " + e.getMessage());
            return FROM + SPACE + "unknown_table";
        }
    }

}
