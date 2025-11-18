package com.capstone.controller;

import com.capstone.dto.DataFrameRequest;
import com.capstone.dto.QueryResponse;
import com.capstone.service.DataFrameService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/convert")
public class DataFrameController {

    private final DataFrameService dataFrameService;

    public DataFrameController(DataFrameService dataFrameService) {
        this.dataFrameService = dataFrameService;
    }

    @PostMapping("/dataframe")
    public ResponseEntity<QueryResponse> convert(@RequestBody DataFrameRequest req) {
        QueryResponse response = dataFrameService.evaluateDataFrame(req.getDataframeCode());
        return ResponseEntity.ok(response);
    }
}
