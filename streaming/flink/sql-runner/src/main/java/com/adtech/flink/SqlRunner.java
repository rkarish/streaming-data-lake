package com.adtech.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

// Executes Flink SQL files in application mode. Replicates the behavior of the
// Flink SQL Client so that the same .sql files used in session mode work here
// without modification.
//
// Supported SQL constructs:
//   - DDL statements (CREATE TABLE/CATALOG, etc.) — executed immediately via executeSql()
//   - SET statements — skipped (configuration is handled by the FlinkDeployment spec)
//   - EXECUTE STATEMENT SET / BEGIN ... END — multiple INSERT statements are accumulated
//     into a StatementSet and submitted as a single Flink job. This lets Flink optimize
//     the execution plan by fusing shared source reads (e.g. kafka_bid_requests is consumed
//     once even when multiple INSERTs read from it).
public class SqlRunner {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: SqlRunner <sql-file> [<sql-file> ...]");
            System.exit(1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        for (String sqlFilePath : args) {
            System.out.println("Executing SQL file: " + sqlFilePath);
            String sql = Files.readString(Path.of(sqlFilePath));
            List<String> statements = parseStatements(sql);

            boolean inStatementSet = false;
            StatementSet statementSet = null;

            for (String stmt : statements) {
                String upper = stmt.toUpperCase().trim();

                // SET statements are handled by the FlinkDeployment spec's flinkConfiguration,
                // so they are not needed here and can be safely skipped.
                if (upper.startsWith("SET ")) {
                    continue;
                }

                // EXECUTE STATEMENT SET begins a block where subsequent INSERT statements
                // are collected rather than executed individually.
                if (upper.equals("EXECUTE STATEMENT SET")) {
                    inStatementSet = true;
                    statementSet = tableEnv.createStatementSet();
                    System.out.println("Starting STATEMENT SET...");
                    continue;
                }

                // BEGIN is part of the EXECUTE STATEMENT SET / BEGIN ... END syntax.
                // It follows EXECUTE STATEMENT SET and has no effect here.
                if (upper.equals("BEGIN")) {
                    continue;
                }

                // END closes the statement set block and submits all accumulated INSERT
                // statements as a single optimized Flink job.
                if (upper.equals("END")) {
                    if (inStatementSet && statementSet != null) {
                        System.out.println("Executing STATEMENT SET...");
                        statementSet.execute();
                        inStatementSet = false;
                        statementSet = null;
                    }
                    continue;
                }

                if (inStatementSet && statementSet != null) {
                    // Inside a statement set: accumulate INSERT statements for batch submission.
                    System.out.println("Adding to STATEMENT SET: " + stmt.substring(0, Math.min(stmt.length(), 80)) + "...");
                    statementSet.addInsertSql(stmt);
                } else {
                    // Outside a statement set: execute DDL (CREATE TABLE, CREATE CATALOG, etc.)
                    // immediately. Flink's executeSql() only accepts single DDL/DML statements.
                    System.out.println("Executing: " + stmt.substring(0, Math.min(stmt.length(), 80)) + "...");
                    tableEnv.executeSql(stmt);
                }
            }
        }

        System.out.println("All SQL files executed successfully.");
    }

    // Parses a Flink SQL file into individual statements. Handles two kinds of
    // statement terminators:
    //   1. Semicolons — standard SQL delimiter for DDL/DML (e.g. CREATE TABLE ...;)
    //   2. Standalone directives — EXECUTE STATEMENT SET, BEGIN, and END appear on
    //      their own line without a trailing semicolon (matching Flink SQL Client syntax).
    // Comments (-- ...) and blank lines are stripped during parsing.
    private static List<String> parseStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (String line : sql.split("\n")) {
            String trimmed = line.trim();

            if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                continue;
            }

            // Standalone directives: these don't use semicolon terminators in Flink SQL.
            // Flush any pending statement and emit the directive as its own statement.
            String upper = trimmed.toUpperCase();
            if (upper.equals("EXECUTE STATEMENT SET") ||
                upper.equals("BEGIN") ||
                upper.equals("END")) {
                String pending = current.toString().trim();
                if (!pending.isEmpty()) {
                    statements.add(pending);
                }
                current = new StringBuilder();
                statements.add(trimmed);
                continue;
            }

            // Accumulate lines into the current statement.
            if (current.length() > 0) {
                current.append("\n");
            }
            current.append(line);

            // A trailing semicolon marks the end of a statement.
            if (trimmed.endsWith(";")) {
                String stmt = current.toString().trim();
                if (stmt.endsWith(";")) {
                    stmt = stmt.substring(0, stmt.length() - 1).trim();
                }
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                current = new StringBuilder();
            }
        }

        String remaining = current.toString().trim();
        if (!remaining.isEmpty()) {
            statements.add(remaining);
        }

        return statements;
    }
}
