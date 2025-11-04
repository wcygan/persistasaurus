/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ExecutionLog {

    public record Invocation(
                             UUID id,
                             int step,
                             Instant timestamp,
                             String className,
                             String methodName,
                             boolean isComplete,
                             int attempts,
                             Object[] parameters,
                             Object returnValue) {
    }

    private static final String DB_URL = "jdbc:sqlite:execution_log.db";
    private static final ExecutionLog INSTANCE = new ExecutionLog();

    private Connection connection;

    private ExecutionLog() {
        setupConnection();
    }

    private void setupConnection() {
        try {
            connection = DriverManager.getConnection(DB_URL);
            connection.setAutoCommit(true);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
                stmt.execute("PRAGMA busy_timeout=5000");
            }
            createTableIfNotExists();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to initialize ExecutionLog", e);
        }
    }

    public static ExecutionLog getInstance() {
        return INSTANCE;
    }

    private void createTableIfNotExists() throws SQLException {
        String createTableSQL = """
                CREATE TABLE IF NOT EXISTS execution_log (
                    id TEXT NOT NULL,
                    step INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    class_name TEXT NOT NULL,
                    method_name TEXT NOT NULL,
                    delay INTEGER,
                    is_complete INTEGER NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 1,
                    parameters BLOB,
                    return_value BLOB,
                    PRIMARY KEY (id, step)
                )
                """;

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
        }
    }

    public synchronized void logInvocationStart(UUID id, int step, String className, String methodName, Duration delay, Object[] parameters) {
        String insertSQL = """
                INSERT INTO execution_log (id, step, timestamp, class_name, method_name, delay, is_complete, attempts, parameters)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id, step)
                DO UPDATE SET attempts = attempts + 1
                """;

        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setString(1, id.toString());
            pstmt.setInt(2, step);
            pstmt.setLong(3, Instant.now().toEpochMilli());
            pstmt.setString(4, className);
            pstmt.setString(5, methodName);

            if (delay != null) {
                pstmt.setLong(6, delay.toMillis());
            }

            pstmt.setInt(7, 0);
            pstmt.setInt(8, 1);
            pstmt.setBytes(9, serializeToBytes(parameters));
            pstmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to log invocation", e);
        }
    }

    public synchronized void logInvocationCompletion(UUID id, int step, Object returnValue) {
        String updateSQL = """
                UPDATE execution_log
                SET is_complete = 1, return_value = ?
                WHERE id = ? AND step = ?
                """;

        if (returnValue instanceof CompletableFuture) {
            try {
                returnValue = ((CompletableFuture<?>) returnValue).get();
            }
            catch (Exception e) {
                throw new RuntimeException("Couldn't get value", e);
            }
        }

        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setBytes(1, serializeToBytes(returnValue));
            pstmt.setString(2, id.toString());
            pstmt.setInt(3, step);
            pstmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to update invocation completion", e);
        }
    }

    public synchronized Invocation getInvocation(UUID id, int step) {
        String selectSQL = """
                SELECT id, step, timestamp, class_name, method_name, is_complete, attempts, parameters, return_value
                FROM execution_log
                WHERE id = ? AND step = ?
                """;

        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, id.toString());
            pstmt.setInt(2, step);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Invocation(
                            UUID.fromString(rs.getString("id")),
                            rs.getInt("step"),
                            Instant.ofEpochMilli(rs.getLong("timestamp")),
                            rs.getString("class_name"),
                            rs.getString("method_name"),
                            rs.getInt("is_complete") == 1,
                            rs.getInt("attempts"),
                            (Object[]) deserializeFromBytes(rs.getBytes("parameters")),
                            deserializeFromBytes(rs.getBytes("return_value")));
                }
                else {
                    return null;
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to retrieve invocation", e);
        }
    }

    public synchronized java.util.List<Invocation> getIncompleteFlows() {
        String selectSQL = """
                SELECT id, step, timestamp, class_name, method_name, is_complete, attempts, parameters, return_value
                FROM execution_log
                WHERE step = 0
                  AND is_complete = 0
                ORDER BY timestamp ASC
                """;

        java.util.List<Invocation> incompleteFlows = new java.util.ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL);
                ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                incompleteFlows.add(new Invocation(
                        UUID.fromString(rs.getString("id")),
                        rs.getInt("step"),
                        Instant.ofEpochMilli(rs.getLong("timestamp")),
                        rs.getString("class_name"),
                        rs.getString("method_name"),
                        rs.getInt("is_complete") == 1,
                        rs.getInt("attempts"),
                        (Object[]) deserializeFromBytes(rs.getBytes("parameters")),
                        deserializeFromBytes(rs.getBytes("return_value"))));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to retrieve incomplete flows", e);
        }

        return incompleteFlows;
    }

    private byte[] serializeToBytes(Object obj) {
        if (obj == null) {
            return null;
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    private Object deserializeFromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize object", e);
        }
    }

    public synchronized void reset() {
        close();
        deleteDatabaseFiles();
        setupConnection();
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                // Log but don't throw
                System.err.println("Failed to close connection: " + e.getMessage());
            }
        }
    }

    private void deleteDatabaseFiles() {
        File dbFile = new File("execution_log.db");
        File walFile = new File("execution_log.db-wal");
        File shmFile = new File("execution_log.db-shm");
        if (dbFile.exists()) {
            dbFile.delete();
        }
        if (walFile.exists()) {
            walFile.delete();
        }
        if (shmFile.exists()) {
            shmFile.delete();
        }
    }
}
