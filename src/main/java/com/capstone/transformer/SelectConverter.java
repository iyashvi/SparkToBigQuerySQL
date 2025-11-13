package com.capstone.transformer;

import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanVisitor;
import org.springframework.stereotype.Component;

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

        switch (node.getNodeType()) {
            case "SELECT":
                selectExpr = node.getExpression();
                break;
            case "FROM":
                fromExpr = node.getExpression();
                break;
            case "JOIN":
                joinExpr = node.getExpression();
                break;
            case "WHERE":
                whereExpr = node.getExpression();
                break;
            case "GROUP BY":
                groupExpr = node.getExpression();
                break;
            case "HAVING":
                havingExpr = node.getExpression();
                break;
            case "ORDER BY":
                orderExpr = node.getExpression();
                break;
            case "LIMIT":
                limitExpr = node.getExpression();
                break;
        }
        // Recursively visit children
        for (SparkPlanNode child : node.getChildren()) {
            visit(child);
        }
    }

    public String getQuery() {
        queryBuilder.setLength(0);

        if (!selectExpr.isEmpty()) queryBuilder.append("SELECT ").append(selectExpr);
        if (!fromExpr.isEmpty()) queryBuilder.append(" FROM ").append(fromExpr);
        if (!joinExpr.isEmpty()) {
            String table1 = "", table2 = "", joinType = "", onCond = "";

            try {
                // Example joinExpr:
                // TABLE1=Employees, TABLE2=Department LEFT OUTER ON Employees.EmployeeID = department.EmployeeID
                int t1s = joinExpr.indexOf("TABLE1=") + 7;
                int t2s = joinExpr.indexOf("TABLE2=");
                int joinTypeStart = joinExpr.indexOf(" ", t2s + 7);
                int onIdx = joinExpr.indexOf(" ON ");

                table1 = joinExpr.substring(t1s, t2s - 2).trim();  // Employees
                table2 = joinExpr.substring(t2s + 7, joinTypeStart).trim(); // Department
                joinType = joinExpr.substring(joinTypeStart, onIdx).trim(); // LEFT OUTER
                onCond = joinExpr.substring(onIdx + 4).trim(); // condition

            } catch (Exception e) {
                System.err.println("JOIN parsing error: " + e.getMessage());
            }

            queryBuilder.append(" FROM ")
                    .append(table1)
                    .append(" ")
                    .append(joinType)
                    .append(" JOIN ")
                    .append(table2)
                    .append(" ON ")
                    .append(onCond);
        }


        if (!whereExpr.isEmpty()) queryBuilder.append(" WHERE ").append(whereExpr);
        if (!groupExpr.isEmpty()) queryBuilder.append(" GROUP BY ").append(groupExpr);
        if (!havingExpr.isEmpty()) queryBuilder.append(" HAVING ").append(havingExpr);
        if (!orderExpr.isEmpty()) queryBuilder.append(" ORDER BY ").append(orderExpr);
        if (!limitExpr.isEmpty()) queryBuilder.append(" LIMIT ").append(limitExpr);

        return queryBuilder.toString().trim();
    }

}
