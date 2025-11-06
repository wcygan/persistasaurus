/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.persistasaurus.Persistasaurus.FlowInstance;
import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.Invocation;
import dev.morling.persistasaurus.internal.ExecutionLog.InvocationStatus;

public class DelayedFlowTest {

    private ExecutionLog executionLog;

    @BeforeEach
    public void setup() {
        executionLog = ExecutionLog.getInstance();
        executionLog.reset();
    }

    @Test
    public void shouldRunDelayedFlow() throws InterruptedException {
        DelayedTestFlow.executionLatch = new CountDownLatch(1);
        DelayedTestFlow.executed = false;

        UUID uuid = UUID.randomUUID();
        FlowInstance<DelayedTestFlow> flow = Persistasaurus.getFlow(DelayedTestFlow.class, uuid);

        // Start the flow - the delayed step should be scheduled
        flow.runAsync(f -> f.runWithDelay());

        // Wait for background worker to execute the delayed flow (max 5 seconds)
        boolean completed = DelayedTestFlow.executionLatch.await(5, TimeUnit.SECONDS);

        assertThat(completed).isTrue();
        assertThat(DelayedTestFlow.executed).isTrue();

        // Verify the flow was completed
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
            Invocation flowInvocation = executionLog.getInvocation(uuid, 0);
            assertThat(flowInvocation).isNotNull();
            return flowInvocation.status() == InvocationStatus.COMPLETE;
        });

        Invocation stepInvocation = executionLog.getInvocation(uuid, 1);
        assertThat(stepInvocation).isNotNull();
        assertThat(stepInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
    }

    @Test
    public void shouldExecuteDelayedFlow() throws InterruptedException, ExecutionException {
        UUID uuid = UUID.randomUUID();
        FlowInstance<DelayedTestFlow> flow = Persistasaurus.getFlow(DelayedTestFlow.class, uuid);

        // Start the flow - the delayed step should be scheduled
        CompletableFuture<String> result = flow.executeAsync(f -> f.executeWithDelay());
        assertThat(result.get()).isEqualTo("It works!");

        // Verify the flow was completed
        Invocation flowInvocation = executionLog.getInvocation(uuid, 0);
        assertThat(flowInvocation).isNotNull();
        assertThat(flowInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);

        Invocation stepInvocation = executionLog.getInvocation(uuid, 1);
        assertThat(stepInvocation).isNotNull();
        assertThat(stepInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
    }

    public static class DelayedTestFlow {
        public static CountDownLatch executionLatch;
        public static boolean executed;

        @Flow
        public void runWithDelay() {
            String delayed = delayedStep();
            System.out.println(delayed);
        }

        @Flow
        public String executeWithDelay() {
            String delayed = delayedStep();
            System.out.println(delayed);
            return delayed;
        }

        @Step(delay = 1) // 1 second delay
        protected String delayedStep() {
            executed = true;
            System.out.println("Delayed step executed!");
            if (executionLatch != null) {
                executionLatch.countDown();
            }

            return "It works!";
        }
    }
}
