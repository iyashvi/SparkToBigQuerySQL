package com.capstone.model;

import com.capstone.parser.PlanVisitor;
import lombok.Data;

import java.util.*;

import static com.capstone.constants.Constants.*;

@Data

public class SparkPlanNode {
    private final String nodeType;
    private String expression;
    private final List<SparkPlanNode> children = new ArrayList<>();

    // JOIN metadata fields
    private String table1;
    private String alias1;
    private String table2;
    private String alias2;
    private String joinType;
    private String joinCondition;

    public SparkPlanNode(String nodeType, String expression) {
        this.nodeType = nodeType;
        this.expression = expression;
    }

    public void addChild(SparkPlanNode child) { this.children.add(child); }


    // Join helpers
    public boolean hasTable1() { return table1 != null; }

    public void setTable1(String table, String alias) {
        this.table1 = table;
        this.alias1 = alias;
    }

    public void setTable2(String table, String alias) {
        this.table2 = table;
        this.alias2 = alias;
    }

    public String getJoinSQL() {
        return table1 + " AS " + alias1 + " " +
                joinType + " JOIN " +
                table2 + " AS " + alias2 +
                " ON " + joinCondition;
    }

    @Override
    public String toString() {
        return nodeType + LEFT_ROUND_BRACKET + expression + RIGHT_ROUND_BRACKET;
    }
}
