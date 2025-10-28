/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.Invocation;

public class IncompleteFlowRecoveryTest {

    private Persistasaurus persistasaurus;

    @BeforeAll
    public static void removeDatabaseFile() {
        ExecutionLog.getInstance().close();
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
        ExecutionLog.getInstance().reset();
    }

    @BeforeEach
    public void setup() {
        persistasaurus = new Persistasaurus();
    }

    @AfterEach
    public void teardown() {
        persistasaurus.shutdown();
    }

    @Test
    public void shouldRecoverIncompleteFlowsOnStartup() throws InterruptedException {
        RecoveryTestFlow.executionLatch = new CountDownLatch(1);
        RecoveryTestFlow.recovered = false;

        UUID uuid = UUID.randomUUID();

        // First execution - create an incomplete flow
        ExecutionLog executionLog = ExecutionLog.getInstance();
        executionLog.logInvocationStart(uuid, 0,
                RecoveryTestFlow.class.getName(),
                "executeFlow",
                null,
                new Object[0]);

        // Verify it's incomplete
        Invocation incompleteFlow = executionLog.getInvocation(uuid, 0);
        assertThat(incompleteFlow).isNotNull();
        assertThat(incompleteFlow.isComplete()).isFalse();

        persistasaurus.recoverIncompleteFlows();

        // Wait for the flow to be executed (max 3 seconds)
        boolean executed = RecoveryTestFlow.executionLatch.await(3, TimeUnit.SECONDS);

        assertThat(executed).isTrue();
        assertThat(RecoveryTestFlow.recovered).isTrue();

        // Verify flow is now complete
        Invocation completedFlow = executionLog.getInvocation(uuid, 0);
        assertThat(completedFlow).isNotNull();
        assertThat(completedFlow.isComplete()).isTrue();
    }

    public static class RecoveryTestFlow {
        public static CountDownLatch executionLatch;
        public static boolean recovered;

        @Flow
        public void executeFlow() {
            recovered = true;
            System.out.println("Recovered flow executed!");
            if (executionLatch != null) {
                executionLatch.countDown();
            }
        }
    }
}
