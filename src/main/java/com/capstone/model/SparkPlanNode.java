package com.capstone.model;

import lombok.Data;

import java.util.*;

@Data
public class SparkPlanNode {
    private final String nodeType;
    private final String expression;
    private final List<SparkPlanNode> children = new ArrayList<>();

    public SparkPlanNode(String nodeType, String expression) {
        this.nodeType = nodeType;
        this.expression = expression;
    }

    public void addChild(SparkPlanNode child) {
        this.children.add(child);
    }
}