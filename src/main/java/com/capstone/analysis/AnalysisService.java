package com.capstone.analysis;

import com.capstone.config.TableCreation;
import com.capstone.dto.AnalysisResponse;
import com.capstone.extractor.SparkPlanExtractor;
import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanWalker;
import com.capstone.parser.SparkPlanParser;
import com.capstone.transformer.SelectConverter;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AnalysisService {

    private final TableCreation tableCreation;
    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;

    public AnalysisService(TableCreation tableCreation, SparkPlanExtractor extractor, SparkPlanParser parser) {
        this.tableCreation = tableCreation;
        this.extractor = extractor;
        this.parser = parser;
    }

    public List<AnalysisResponse> analysisReport(String query) {
        List<AnalysisResponse> responses = new ArrayList<>();
        AnalysisResponse response = new AnalysisResponse();

        tableCreation.createDemoTempViews(); // Test data

        String logical = extractor.extractLogicalPlan(query);

        System.out.println("Logical Plan ================= " + logical);
        SparkPlanNode root = parser.parse(logical);

        if (Objects.isNull(root)) {
            throw new IllegalStateException("Parsed plan is empty — no valid root node found.");
        }
        // 3. Walk nodes using visitor
        PlanWalker walker = new PlanWalker();
        SelectConverter converter = new SelectConverter();
        walker.walk(root, converter);

        response = converter.getAnalysis();
        response.setSourceSQL(query);

        responses.add(response);
        return responses;
    }
}
