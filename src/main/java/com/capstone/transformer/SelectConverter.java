package com.capstone.transformer;

import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanVisitor;
import org.springframework.stereotype.Component;

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
    }

    public String getQuery() {
        queryBuilder.setLength(0);

        if (!selectExpr.isEmpty()) queryBuilder.append("SELECT ").append(selectExpr);
        if (!fromExpr.isEmpty()) queryBuilder.append(" FROM ").append(fromExpr);
        if (!whereExpr.isEmpty()) queryBuilder.append(" WHERE ").append(whereExpr);
        if (!groupExpr.isEmpty()) queryBuilder.append(" GROUP BY ").append(groupExpr);
        if (!havingExpr.isEmpty()) queryBuilder.append(" HAVING ").append(havingExpr);
        if (!orderExpr.isEmpty()) queryBuilder.append(" ORDER BY ").append(orderExpr);
        if (!limitExpr.isEmpty()) queryBuilder.append(" LIMIT ").append(limitExpr);

        return queryBuilder.toString().trim();
    }

}

