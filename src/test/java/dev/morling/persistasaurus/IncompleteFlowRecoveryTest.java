/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.InvocationStatus;
import dev.morling.persistasaurus.internal.Invocation;

public class IncompleteFlowRecoveryTest {

    @BeforeEach
    public void setup() {
        ExecutionLog.getInstance().reset();
    }

    @Test
    public void shouldRecoverIncompleteFlow() throws InterruptedException {
        RecoveryTestFlow.executionLatch = new CountDownLatch(1);
        RecoveryTestFlow.recovered = false;

        UUID uuid = UUID.randomUUID();

        // First execution - create an incomplete flow
        ExecutionLog executionLog = ExecutionLog.getInstance();
        executionLog.logInvocationStart(uuid, 0,
                RecoveryTestFlow.class.getName(),
                "executeFlow",
                null,
                InvocationStatus.PENDING,
                new Object[0]);

        // Verify it's incomplete
        Invocation incompleteFlow = executionLog.getInvocation(uuid, 0);
        assertThat(incompleteFlow).isNotNull();
        assertThat(incompleteFlow.status()).isEqualTo(InvocationStatus.PENDING);

        Persistasaurus.recoverIncompleteFlows();

        // Wait for the flow to be executed (max 3 seconds)
        boolean executed = RecoveryTestFlow.executionLatch.await(3, TimeUnit.SECONDS);

        assertThat(executed).isTrue();
        assertThat(RecoveryTestFlow.recovered).isTrue();

        // Verify flow is now complete
        Invocation completedFlow = executionLog.getInvocation(uuid, 0);
        assertThat(completedFlow).isNotNull();
        assertThat(completedFlow.status()).isEqualTo(InvocationStatus.COMPLETE);
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
