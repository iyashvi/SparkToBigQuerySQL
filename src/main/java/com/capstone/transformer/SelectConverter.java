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

    public String getQuery() {
        queryBuilder.setLength(0);

        if (!selectExpr.isEmpty()) queryBuilder.append(SELECT+SPACE).append(selectExpr);
        if (!fromExpr.isEmpty()) queryBuilder.append(SPACE+FROM+SPACE).append(fromExpr);
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

            queryBuilder.append(SPACE+FROM+SPACE)
                    .append(table1)
                    .append(SPACE)
                    .append(joinType)
                    .append(SPACE+JOIN+SPACE)
                    .append(table2)
                    .append(SPACE+ON+SPACE)
                    .append(onCond);
        }


        if (!whereExpr.isEmpty()) queryBuilder.append(SPACE+WHERE+SPACE).append(whereExpr);
        if (!groupExpr.isEmpty()) queryBuilder.append(SPACE+GROUP_BY+SPACE).append(groupExpr);
        if (!havingExpr.isEmpty()) queryBuilder.append(SPACE+HAVING+SPACE).append(havingExpr);
        if (!orderExpr.isEmpty()) queryBuilder.append(SPACE+ORDER_BY+SPACE).append(orderExpr);
        if (!limitExpr.isEmpty()) queryBuilder.append(SPACE+LIMIT+SPACE).append(limitExpr);

        return queryBuilder.append(SEMI_COLON).toString().trim();
    }

}
