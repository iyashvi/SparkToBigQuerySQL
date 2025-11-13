package com.capstone.dto;

import lombok.Data;

import java.util.List;

@Data
public class FileQueryResponse {
    private String bigQuerySql;
    private String logicalPlanText;
    private List<String> warnings;
}
