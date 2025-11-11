package com.capstone.transformer;

import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanVisitor;
import org.springframework.stereotype.Component;

@Component
public class SelectConverter extends PlanVisitor {

    private final StringBuilder queryBuilder = new StringBuilder();

    @Override
    public void visit(SparkPlanNode node) {
        switch (node.getNodeType()) {
            case "SELECT":
                queryBuilder.append("SELECT ").append(node.getExpression());
                break;
            case "FROM":
                queryBuilder.append(" FROM ").append(node.getExpression());
                break;
        }
    }

    public String getQuery() {
        return queryBuilder.toString();
    }
}

