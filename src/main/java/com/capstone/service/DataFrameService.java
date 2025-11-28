package com.capstone.service;

import com.capstone.config.TableCreation;
import com.capstone.dto.FileQueryResponse;
import com.capstone.extractor.SparkPlanExtractor;
import com.capstone.model.SparkPlanNode;
import com.capstone.parser.PlanWalker;
import com.capstone.parser.SparkPlanParser;
import com.capstone.transformer.SelectConverter;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.capstone.constants.Constants.*;

@Service
public class DataFrameService {

    private final TableCreation tableCreation;
    private final SparkPlanExtractor extractor;
    private final SparkPlanParser parser;

    public DataFrameService(TableCreation tableCreation, SparkPlanExtractor extractor, SparkPlanParser parser) {
        this.tableCreation = tableCreation;
        this.extractor = extractor;
        this.parser = parser;
    }

    public FileQueryResponse evaluateDataFrame(String dfCode) {
        FileQueryResponse response = new FileQueryResponse();
        List<String> warnings = new ArrayList<>();

        try {
            if (Objects.isNull(dfCode) || dfCode.isBlank()) {
                response.setBigQuerySql("/* Empty DataFrame code */");
                return response;
            }

            // STEP 1: Convert to Spark SQL
            String sparkSql = convertDataFrameToSql(dfCode.replace("\\", ""));
            System.out.println("Generated Spark SQL: " + sparkSql);

            tableCreation.createDemoTempViews(); // Test data

            // STEP 4: Extract plans
            String logical = extractor.extractLogicalPlan(sparkSql);
            response.setLogicalPlanText(logical);

            System.out.println("Logical Plan ================= " + logical);

            // STEP 5: PARSE + CONVERT
            SparkPlanNode root = parser.parse(logical);
            PlanWalker walker = new PlanWalker();
            SelectConverter converter = new SelectConverter();
            walker.walk(root, converter);

            response.setBigQuerySql(converter.getQuery());
        }
        catch (Exception e) {
            warnings.add("Error while evaluating DataFrame: " + e.getMessage());
            response.setBigQuerySql("/* translation failed: " + e.getMessage() + " */");
        }

        response.setWarnings(warnings);
        return response;
    }
    private String convertDataFrameToSql(String code) {
        code = code.replace("\n", " ").replace("\\", "").trim();


        // Table info
        String baseTable = null;
        String baseAlias = null;
        String joinTable = null;
        String joinAlias = null;
        String joinColumn = null;
        String joinType = "INNER JOIN";

        // Columns
        List<String> selectCols = new ArrayList<>();
        List<String> aggCols = new ArrayList<>();
        List<String> groupByCols = new ArrayList<>();

        // Conditions
        String whereCond = null;
        String havingCond = null;
        String orderBy = null;
        String limit = null;
        String offset = null;


        // BASE TABLE
        Matcher base = Pattern.compile("spark\\.table\\(['\"](.*?)['\"]\\)(?:\\.alias\\(['\"](.*?)['\"]\\))?").matcher(code);
        if (base.find()) {
            baseTable = base.group(1);
            baseAlias = base.groupCount() >= 2 ? base.group(2) : null;
        }


        // SELECT columns
        Matcher sel = Pattern.compile("select\\((.*?)\\)").matcher(code);
        if (sel.find()) {
            selectCols = Arrays.stream(sel.group(1).split(","))
                    .map(String::trim)
                    .map(s -> s.replaceAll("^['\"]|['\"]$", "")) // remove quotes
                    .toList();
        }

        // WHERE
        Matcher wh = Pattern.compile("filter\\((.*?)\\)").matcher(code);
        if (wh.find()) whereCond = wh.group(1).replaceAll("^['\"]|['\"]$", "").trim();


        // groupBy / agg / having
        String[] parts = code.split("\\.");
        for (String p : parts) {
            p = p.trim();
            if (p.startsWith("groupBy(")) {
                groupByCols = Arrays.asList(extractBalanced(p).split(",")).stream()
                        .map(String::trim)
                        .map(s -> s.replaceAll("^['\"]|['\"]$", "")) // remove quotes
                        .toList();
            } else if (p.startsWith("agg(")) {
                aggCols = Arrays.asList(extractBalanced(p).split(",")).stream()
                        .map(String::trim)
                        .map(s -> s.matches("^['\"].*['\"]$") ? s.substring(1, s.length() - 1) : s)
                        .toList();
            } else if (p.startsWith("having(")) {
                havingCond = extractBalanced(p).trim();
                havingCond = havingCond.replaceAll("'(\\w+\\(.*?\\))'", "$1"); // remove quotes around functions
            }
        }

        // JOIN
        Matcher join = Pattern.compile("join\\((.*?),\\s*['\"](.*?)['\"](?:,\\s*['\"](.*?)['\"])?\\)").matcher(code);
        if (join.find()) {
            joinColumn = join.group(2).trim();
            joinType = join.groupCount() >= 3 && join.group(3) != null ? join.group(3).toUpperCase() + " JOIN" : "INNER JOIN";

            Matcher jt = Pattern.compile("spark\\.table\\(['\"](.*?)['\"]\\)(?:\\.alias\\(['\"](.*?)['\"]\\))?").matcher(join.group(1));
            if (jt.find()) {
                joinTable = jt.group(1);
                joinAlias = jt.groupCount() >= 2 ? jt.group(2) : null;
            }
        }

        // ORDER BY
        Matcher ord = Pattern.compile("orderBy\\((.*?)\\)").matcher(code);
        if (ord.find()) orderBy = ord.group(1).replaceAll("^['\"]|['\"]$", "").trim();


        // LIMIT
        Matcher lim = Pattern.compile("limit\\((.*?)\\)").matcher(code);
        if (lim.find()) limit = lim.group(1).trim();

        // OFFSET
        Matcher off = Pattern.compile("offset\\((.*?)\\)").matcher(code);
          if (off.find()) offset = off.group(1).trim();


        // BUILD SQL
        StringBuilder sql = new StringBuilder("SELECT ");

        if (!aggCols.isEmpty()) sql.append(String.join(", ", aggCols));
        else if (!selectCols.isEmpty()) sql.append(String.join(", ", selectCols));
        else sql.append("*");

        sql.append(" FROM ").append(baseTable);
        if (baseAlias != null && !baseAlias.isBlank()) sql.append(" ").append(baseAlias);

        // JOIN
        if (joinTable != null) {
            sql.append(" ").append(joinType).append(" ").append(joinTable);
            if (joinAlias != null && !joinAlias.isBlank()) sql.append(" ").append(joinAlias);

            String leftTable = baseAlias != null ? baseAlias : baseTable;
            String rightTable = joinAlias != null ? joinAlias : joinTable;

            sql.append(" ON ").append(leftTable).append(".").append(joinColumn)
                    .append(" = ").append(rightTable).append(".").append(joinColumn);
        }


        // WHERE
        if (whereCond != null) sql.append(" WHERE ").append(whereCond);

        // GROUP BY
        if (!groupByCols.isEmpty()) sql.append(" GROUP BY ").append(String.join(", ", groupByCols));

        // HAVING
        if (havingCond != null) sql.append(" HAVING ").append(havingCond);

        // ORDER BY
        if (orderBy != null) sql.append(" ORDER BY ").append(orderBy);

        //LIMIT + OFFSET
        if (limit != null) sql.append(" LIMIT ").append(limit);
        if (offset != null) sql.append(" OFFSET ").append(offset);


        sql.append(";");
        return sql.toString();
    }

    private String extractBalanced(String expr) {
        int start = expr.indexOf(LEFT_ROUND_BRACKET) + 1;
        int end = findClosingParen(expr, start - 1);
        if (start < 0 || end < 0 || end <= start) return "";
        return expr.substring(start, end)
                .replace(QUOTES, "")
                .replace(SINGLE_INVERTED_COMMA, "")
                .trim();
    }

    private int findClosingParen(String str, int openPos) {
        int depth = 0;
        for (int i = openPos; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            if (depth == 0) return i;
        }
        return -1;
    }

}