package com.capstone.model;

import lombok.Data;
import java.util.*;

@Data
public class PlanNode {
    private String type; // e.g., Project, Filter, Join, Aggregate, Relation, Limit
    private String raw; // raw line
    private List<PlanNode> children = new ArrayList<>();
    private int indentLevel = 0; // helpful for parser
}