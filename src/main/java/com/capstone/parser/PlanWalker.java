package com.capstone.parser;

import com.capstone.model.SparkPlanNode;

import java.util.Objects;

public class PlanWalker {

    public void walk(SparkPlanNode node, PlanVisitor visitor) {
        if (Objects.isNull(node) || Objects.isNull(visitor)) return;

        visitor.visit(node);
        if (node.getChildren() != null) {
            for (SparkPlanNode child : node.getChildren()) {
                walk(child, visitor);
            }
        }
    }
}
