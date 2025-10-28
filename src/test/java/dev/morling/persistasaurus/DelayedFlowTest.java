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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.Invocation;

public class DelayedFlowTest {

    private ExecutionLog executionLog;
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
        executionLog = ExecutionLog.getInstance();
        persistasaurus = new Persistasaurus();
    }

    @AfterEach
    public void teardown() {
        persistasaurus.shutdown();
    }

    @Test
    public void shouldExecuteDelayedFlow() throws InterruptedException {
        DelayedTestFlow.executionLatch = new CountDownLatch(1);
        DelayedTestFlow.executed = false;

        UUID uuid = UUID.randomUUID();
        DelayedTestFlow flow = persistasaurus.getFlow(DelayedTestFlow.class, uuid);

        // Start the flow - the delayed step should be scheduled
        flow.runWithDelay();

        // Wait for background worker to execute the delayed flow (max 5 seconds)
        boolean completed = DelayedTestFlow.executionLatch.await(5, TimeUnit.SECONDS);

        assertThat(completed).isTrue();
        assertThat(DelayedTestFlow.executed).isTrue();

        // Verify the flow was completed
        Invocation flowInvocation = executionLog.getInvocation(uuid, 0);
        assertThat(flowInvocation).isNotNull();
        assertThat(flowInvocation.isComplete()).isTrue();

        Invocation stepInvocation = executionLog.getInvocation(uuid, 1);
        assertThat(stepInvocation).isNotNull();
        assertThat(stepInvocation.isComplete()).isTrue();
    }

    public static class DelayedTestFlow {
        public static CountDownLatch executionLatch;
        public static boolean executed;

        @Flow
        public void runWithDelay() {
            CompletableFuture<String> delayed = delayedStep();
            if (!delayed.isDone()) {
                return;
            }

            try {
                System.out.println(delayed.get());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Step(delay = 1) // 1 second delay
        protected CompletableFuture<String> delayedStep() {
            executed = true;
            System.out.println("Delayed step executed!");
            if (executionLatch != null) {
                executionLatch.countDown();
            }

            return CompletableFuture.completedFuture("It works!");
        }
    }
}
