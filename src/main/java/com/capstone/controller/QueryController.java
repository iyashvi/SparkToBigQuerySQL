package com.capstone.controller;

import com.capstone.constants.URLConstants;
import com.capstone.dto.QueryRequest;
import com.capstone.dto.QueryResponse;
import com.capstone.service.SparkPlanService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = URLConstants.CONVERT_URL)
public class QueryController {
    private final SparkPlanService sparkPlanService;
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryController.class);

    public QueryController(SparkPlanService sparkPlanService) {
        this.sparkPlanService = sparkPlanService;
    }

    @PostMapping("/query")
    public ResponseEntity<QueryResponse> translate(@RequestBody QueryRequest request) throws Exception {
        String query = request.getSparkSql();
        LOGGER.info("======== BigQuery SQL Conversion for Spark SQL: {} ========", query);
        QueryResponse resp = sparkPlanService.translateSql(query);
        return ResponseEntity.ok(resp);
    }
}
