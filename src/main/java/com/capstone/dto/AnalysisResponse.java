package com.capstone.dto;

import lombok.Data;

import java.util.AbstractMap.SimpleEntry;

@Data
public class AnalysisResponse {

    private String sourceSQL;
    private String complexity;
    private String joinClauses;
    private SimpleEntry<String, Integer> whereClauses;
    private SimpleEntry<String, Integer> functions;
    private SimpleEntry<String, Integer> groupByClauses;
    private SimpleEntry<String, Integer> orderByClauses;
    private String subquery;
    private String status;
    private String sourceTables;
    private String targetTables;
    private String manualEffortHours;
    private String automatedEffortHours;
    private String targetSQL;

}
