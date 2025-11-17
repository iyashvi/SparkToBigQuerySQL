package com.capstone.controller;

import com.capstone.constants.URLConstants;
import com.capstone.dto.FileQueryResponse;
import com.capstone.dto.QueryRequest;
import com.capstone.dto.QueryResponse;
import com.capstone.service.SQLFileService;
import com.capstone.service.SparkPlanService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

@RestController
@RequestMapping(value = URLConstants.CONVERT_URL)
public class QueryController {
    private final SparkPlanService sparkPlanService;
    private final SQLFileService sqlFileService;
    private static final Logger log = LoggerFactory.getLogger(QueryController.class);

    public QueryController(SparkPlanService sparkPlanService, SQLFileService sqlFileService) {
        this.sparkPlanService = sparkPlanService;
        this.sqlFileService = sqlFileService;
    }

    @PostMapping("/query")
    public ResponseEntity<QueryResponse> translateQuery(@RequestBody QueryRequest request) throws Exception {
        String query = request.getSparkSql();
        log.info("======== BigQuery SQL Conversion for Spark SQL: {} ========", query);
        QueryResponse resp = sparkPlanService.translateSql(query);
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/sqlFile")
    public ResponseEntity<List<FileQueryResponse>> translateSqlFile(@RequestParam("file") MultipartFile file) throws Exception {
        if (file == null || file.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Received file: {}", file.getOriginalFilename());
        log.info("File size: {}", file.getSize());
        try {
            File tempFile = File.createTempFile("upload_", ".sql");
            file.transferTo(tempFile);
            List<FileQueryResponse> response = sqlFileService.extractQueriesFromFile(tempFile.getAbsolutePath());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
