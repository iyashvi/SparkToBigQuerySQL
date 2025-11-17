package com.capstone.transformer;

import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanVisitor;
import static com.capstone.constants.Constants.*;
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
            case SELECT:
                selectExpr = node.getExpression();
                break;
            case FROM:
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


        if (!selectExpr.isEmpty()) {
            queryBuilder.append("SELECT ").append(selectExpr.replaceAll("#\\d+", ""));
        } else {
            queryBuilder.append("SELECT *");
        }
// FROM + JOIN
        if (!joinTable1.isEmpty() && !joinTable2.isEmpty()) {
            // Use JOIN tables
            queryBuilder.append(" FROM ").append(joinTable1);
            if (joinAlias1 != null && !joinAlias1.isEmpty() && !joinAlias1.equalsIgnoreCase("null")) {
                queryBuilder.append(" AS ").append(joinAlias1);
            }
            queryBuilder.append(" ").append(joinType).append(" ").append(joinTable2);
            if (joinAlias2 != null && !joinAlias2.isEmpty() && !joinAlias2.equalsIgnoreCase("null")) {
                queryBuilder.append(" AS ").append(joinAlias2);
            }
            if (joinOn != null && !joinOn.isEmpty()) {
                joinOn = joinOn.replaceAll("#\\d+", "");
                queryBuilder.append(" ON ").append(joinOn);
            }
        } else if (!fromExpr.isEmpty()) {
            // Only FROM table, no JOIN
            queryBuilder.append(" FROM ").append(fromExpr.replaceAll("#\\d+", ""));
        }

//        if (!selectExpr.isEmpty()) queryBuilder.append("SELECT ").append(selectExpr);
//        else if (!fromExpr.isEmpty()) queryBuilder.append("SELECT * ");
//        if (!joinTable1.isEmpty()) {
//            queryBuilder.append(" FROM ")
//                    .append(joinTable1).append(" ").append(joinAlias1);
//        }
//        else if (!fromExpr.isEmpty()) {
//            queryBuilder.append(" FROM ").append(fromExpr);
//        }
//        if (!joinTable1.isEmpty() && !joinTable2.isEmpty()) {
//            queryBuilder.append(" FROM ")
//                    .append(joinTable1);
//            if (joinAlias1 != null && !joinAlias1.isEmpty() && !joinAlias1.equalsIgnoreCase("null")) {
//                queryBuilder.append(" AS ").append(joinAlias1);
//            }
//            queryBuilder.append(" ").append(joinType).append(" ")
//                    .append(joinTable2);
//            if (joinAlias2 != null && !joinAlias2.isEmpty() && !joinAlias2.equalsIgnoreCase("null")) {
//                queryBuilder.append(" AS ").append(joinAlias2);
//            }
//            if (joinOn != null && !joinOn.isEmpty()) {
//                joinOn = joinOn.replaceAll("#\\d+", ""); // clean Spark #numbers
//                queryBuilder.append(" ON ").append(joinOn);
//            }
//        }
        if (!whereExpr.isEmpty()) queryBuilder.append(" WHERE ").append(whereExpr);
        if (!groupExpr.isEmpty()) queryBuilder.append(" GROUP BY ").append(groupExpr);
        if (!havingExpr.isEmpty()) queryBuilder.append(" HAVING ").append(havingExpr);
        if (!orderExpr.isEmpty()) queryBuilder.append(" ORDER BY ").append(orderExpr);
        if (!limitExpr.isEmpty()) queryBuilder.append(" LIMIT ").append(limitExpr);

        return queryBuilder.append(SEMI_COLON).toString().trim();
    }

}
