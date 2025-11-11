package com.capstone.parser;


import com.capstone.model.SparkPlanNode;

public class PlanWalker {

    public void walk(SparkPlanNode node, PlanVisitor visitor) {
        visitor.visit(node);
        for (SparkPlanNode child : node.getChildren()) {
            walk(child, visitor);
        }
    }
}
