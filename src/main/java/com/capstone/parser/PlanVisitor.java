package com.capstone.parser;

import com.capstone.model.SparkPlanNode;

public abstract class PlanVisitor {
    public abstract void visit(SparkPlanNode node);
}
