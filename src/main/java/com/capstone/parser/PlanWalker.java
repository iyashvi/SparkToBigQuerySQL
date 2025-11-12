package com.capstone.parser;


import com.capstone.model.SparkPlanNode;

public class PlanWalker {

    public void walk(SparkPlanNode node, PlanVisitor visitor) {
        if (node == null || visitor == null) return;

        visitor.visit(node);
        if (node.getChildren() != null) {
            for (SparkPlanNode child : node.getChildren()) {
                walk(child, visitor);
            }
        }
    }
}
