package com.capstone.service;

import com.capstone.dto.FileQueryResponse;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class DfFileService {

    private static final Logger log = LoggerFactory.getLogger(DfFileService.class);

    private final DataFrameService dataFrameService;

    public DfFileService(DataFrameService dataFrameService) {
        this.dataFrameService = dataFrameService;
    }

    // Helper to read SQL file content into String
    public static String readDfFile(String filePath) throws IOException {
        log.info("Reading DF file content from: {}", filePath);
        try {
            return Files.readString(Paths.get(filePath));
        } catch (IOException e) {
            log.error("FATAL: Could not read DF file at {}.", filePath, e);
            throw new UncheckedIOException("Failed to read DF file: " + filePath, e);
        }
    }

    public List<FileQueryResponse> extractQueriesFromFile(String filePath) throws IOException {
        log.info("DF file path: {}", filePath);

        List<FileQueryResponse> responses = new ArrayList<>();
        String fileContent = readDfFile(filePath);
        String[] queries = fileContent.split("(?<=;)");

        for (String query : queries) {
            String trimmedQuery = query.trim();
            if (!trimmedQuery.isEmpty()) {
                responses.add(dataFrameService.evaluateDataFrame(trimmedQuery));
            }
        }
        return responses;
    }
}
