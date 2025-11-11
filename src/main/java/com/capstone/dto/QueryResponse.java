package com.capstone.dto;

import lombok.Data;
import java.util.List;

@Data
public class QueryResponse {
    private String bigQuerySql;
    private String logicalPlanText;
    private String optimizedPlanText;
    private String physicalPlanText;
    private List<String> warnings;
}