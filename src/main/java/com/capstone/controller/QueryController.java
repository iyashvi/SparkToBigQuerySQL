package com.capstone.controller;

import com.capstone.constants.URLConstants;
import com.capstone.dto.*;
import com.capstone.service.DataFrameService;
import com.capstone.service.DfFileService;
import com.capstone.service.SQLFileService;
import com.capstone.service.SparkPlanService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.*;

@RestController
@RequestMapping(value = URLConstants.CONVERT_URL)
public class QueryController {

    private final SparkPlanService sparkPlanService;
    private final SQLFileService sqlFileService;
    private final DataFrameService dataFrameService;
    private final DfFileService dfFileService;
    private static final Logger log = LoggerFactory.getLogger(QueryController.class);

    public QueryController(SparkPlanService sparkPlanService, SQLFileService sqlFileService, DataFrameService dataFrameService, DfFileService dfFileService, AnalysisService analysisService) {
        this.sparkPlanService = sparkPlanService;
        this.sqlFileService = sqlFileService;
        this.dataFrameService = dataFrameService;
        this.dfFileService = dfFileService;
    }

    // Spark SQL to BigQuery
    @PostMapping("/query")
    public ResponseEntity<FileQueryResponse> translateQuery(@RequestBody QueryRequest request) throws Exception {
        String query = request.getSparkSql();
        log.info("======== BigQuery SQL Conversion for Spark SQL: {} ========", query);
        FileQueryResponse resp = sparkPlanService.translateSql(query);
        return ResponseEntity.ok(resp);
    }

    // SQL File
    @PostMapping("/sqlFile")
    public ResponseEntity<List<FileQueryResponse>> translateSqlFile(@RequestParam("file") MultipartFile file) throws Exception {
        if (Objects.isNull(file) || file.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Received file: {}", file.getOriginalFilename());
        try {
            File tempFile = File.createTempFile("upload_", ".sql");
            file.transferTo(tempFile);
            List<FileQueryResponse> response = sqlFileService.extractQueriesFromFile(tempFile.getAbsolutePath());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Spark DF to BigQuery
    @PostMapping("/dataframe")
    public ResponseEntity<FileQueryResponse> convert(@RequestBody DataFrameRequest req) {
        FileQueryResponse response = dataFrameService.evaluateDataFrame(req.getDataframeCode());
        return ResponseEntity.ok(response);
    }

    // DFCode File
    @PostMapping("/dfFile")
    public ResponseEntity<List<FileQueryResponse>> translateDfFile(@RequestParam("file") MultipartFile file) throws Exception {
        if (Objects.isNull(file) || file.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Received file: {}", file.getOriginalFilename());
        try {
            File tempFile = File.createTempFile("upload_", ".txt");
            file.transferTo(tempFile);
            List<FileQueryResponse> response = dfFileService.extractQueriesFromFile(tempFile.getAbsolutePath());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
