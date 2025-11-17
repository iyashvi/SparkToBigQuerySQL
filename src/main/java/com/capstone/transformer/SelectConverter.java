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
    private String joinTable1 = "";
    private String joinType = "";
    private String joinTable2 = "";
    private String joinAlias1 = "";
    private String joinAlias2 = "";
    private String joinOn = "";

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
                // --- JOIN EXTRACTION FIX HERE ---
                joinTable1 = node.getTable1();
                joinAlias1 = node.getAlias1();
                joinTable2 = node.getTable2();
                joinAlias2 = node.getAlias2();
                joinType   = node.getJoinType();
                joinOn     = node.getJoinCondition();
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
        else if (!fromExpr.isEmpty()) queryBuilder.append("SELECT *");
        if (!joinTable1.isEmpty()) {
            queryBuilder.append(" FROM ").append(joinTable1);
            if (!joinAlias1.isEmpty()) queryBuilder.append(" AS ").append(joinAlias1);
        } else if (!fromExpr.isEmpty()) {
            queryBuilder.append(" FROM ").append(fromExpr);
        }

        if (!joinTable1.isEmpty() && !joinTable2.isEmpty()) {
            queryBuilder.append(" ").append(joinType)
                    .append(" ").append(joinTable2);
            if (!joinAlias2.isEmpty()) queryBuilder.append(" AS ").append(joinAlias2);
            if (!joinOn.isEmpty()) queryBuilder.append(" ON ").append(joinOn);
        }
        if (!whereExpr.isEmpty()) queryBuilder.append(" WHERE ").append(whereExpr);
        if (!groupExpr.isEmpty()) queryBuilder.append(" GROUP BY ").append(groupExpr);
        if (!havingExpr.isEmpty()) queryBuilder.append(" HAVING ").append(havingExpr);
        if (!orderExpr.isEmpty()) queryBuilder.append(" ORDER BY ").append(orderExpr);
        if (!limitExpr.isEmpty()) queryBuilder.append(" LIMIT ").append(limitExpr);

        return queryBuilder.toString().trim();
    }

}
