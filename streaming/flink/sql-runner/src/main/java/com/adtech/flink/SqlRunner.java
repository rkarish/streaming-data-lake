package com.adtech.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;

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

            for (String statement : sql.split(";")) {
                String trimmed = statement.trim();
                if (!trimmed.isEmpty()) {
                    System.out.println("Executing: " + trimmed.substring(0, Math.min(trimmed.length(), 80)) + "...");
                    tableEnv.executeSql(trimmed);
                }
            }
        }

        System.out.println("All SQL files executed successfully.");
    }
}
