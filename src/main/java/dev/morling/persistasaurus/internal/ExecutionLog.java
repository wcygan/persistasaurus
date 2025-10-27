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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class ExecutionLog {

    public record Invocation(
                             UUID id,
                             int step,
                             long timestamp,
                             String className,
                             String methodName,
                             boolean isComplete,
                             int attempts,
                             Object[] parameters,
                             Object returnValue) {
    }

    private static final String DB_URL = "jdbc:sqlite:execution_log.db";
    private Connection connection;

    public ExecutionLog() {
        try {
            connection = DriverManager.getConnection(DB_URL);
            createTableIfNotExists();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to initialize ExecutionLog", e);
        }
    }

    private void createTableIfNotExists() throws SQLException {
        String createTableSQL = """
                CREATE TABLE IF NOT EXISTS execution_log (
                    id TEXT NOT NULL,
                    step INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    class_name TEXT NOT NULL,
                    method_name TEXT NOT NULL,
                    is_complete INTEGER,
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

    public void logInvocationStart(UUID id, int step, String className, String methodName, Object[] parameters) {
        String insertSQL = """
                INSERT INTO execution_log (id, step, timestamp, class_name, method_name, is_complete, attempts, parameters)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id, step)
                DO UPDATE SET attempts = attempts + 1
                """;

        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setString(1, id.toString());
            pstmt.setInt(2, step);
            pstmt.setLong(3, System.currentTimeMillis());
            pstmt.setString(4, className);
            pstmt.setString(5, methodName);
            pstmt.setInt(6, 0);
            pstmt.setInt(7, 1);
            pstmt.setBytes(8, serializeToBytes(parameters));
            pstmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to log invocation", e);
        }
    }

    public void logInvocationCompletion(UUID id, int step, Object returnValue) {
        String updateSQL = """
                UPDATE execution_log
                SET is_complete = 1, return_value = ?
                WHERE id = ? AND step = ?
                """;

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

    public Invocation getInvocation(UUID id, int step) {
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
                            rs.getLong("timestamp"),
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

    public void close() {
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
}
