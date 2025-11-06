/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.persistasaurus.Persistasaurus.FlowInstance;
import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.Invocation;
import dev.morling.persistasaurus.internal.ExecutionLog.InvocationStatus;

public class PersistasaurusTest {

    private ExecutionLog executionLog;

    @BeforeEach
    public void setup() {
        executionLog = ExecutionLog.getInstance();
        executionLog.reset();
    }

    @Test
    public void shouldRunFlowSuccessfully() {
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        UUID uuid = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
        flow.run(f -> f.sayHello());

        // Verify the flow method was logged
        Invocation flowInvocation = executionLog.getInvocation(uuid, 0);
        assertThat(flowInvocation).isNotNull();
        assertThat(flowInvocation.methodName()).isEqualTo("sayHello");
        assertThat(flowInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
        assertThat(flowInvocation.attempts()).isEqualTo(1);

        // Verify all step invocations were logged (5 iterations)
        for (int i = 0; i < 5; i++) {
            Invocation stepInvocation = executionLog.getInvocation(uuid, i + 1);
            assertThat(stepInvocation).isNotNull();
            assertThat(stepInvocation.methodName()).isEqualTo("say");
            assertThat(stepInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
            assertThat(stepInvocation.attempts()).isEqualTo(1);
            assertThat(stepInvocation.parameters()).hasSize(2);
            assertThat(stepInvocation.parameters()[0]).isEqualTo("World");
            assertThat(stepInvocation.parameters()[1]).isEqualTo(i);
            assertThat(stepInvocation.returnValue()).isEqualTo(i);
        }
    }

    @Test
    public void shouldExecuteFlowSuccessfully() {
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        UUID uuid = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
        int sum = flow.execute(f -> f.sayHello());

        assertThat(sum).isEqualByComparingTo(10);

        // Verify the flow method was logged
        Invocation flowInvocation = executionLog.getInvocation(uuid, 0);
        assertThat(flowInvocation).isNotNull();
        assertThat(flowInvocation.methodName()).isEqualTo("sayHello");
        assertThat(flowInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
        assertThat(flowInvocation.attempts()).isEqualTo(1);

        // Verify all step invocations were logged (5 iterations)
        for (int i = 0; i < 5; i++) {
            Invocation stepInvocation = executionLog.getInvocation(uuid, i + 1);
            assertThat(stepInvocation).isNotNull();
            assertThat(stepInvocation.methodName()).isEqualTo("say");
            assertThat(stepInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
            assertThat(stepInvocation.attempts()).isEqualTo(1);
            assertThat(stepInvocation.parameters()).hasSize(2);
            assertThat(stepInvocation.parameters()[0]).isEqualTo("World");
            assertThat(stepInvocation.parameters()[1]).isEqualTo(i);
            assertThat(stepInvocation.returnValue()).isEqualTo(i);
        }
    }

    @Test
    public void shouldReplayCompletedStepsOnRetry() {
        HelloWorldFlow.FAIL_ON_COUNT = 3;
        UUID uuid = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);

        // First execution should fail at step 4 (iteration 3)
        assertThatThrownBy(() -> flow.run(f -> f.sayHello()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("I don't like this count: 3");

        // Verify first 3 iterations completed
        for (int i = 0; i < 3; i++) {
            Invocation stepInvocation = executionLog.getInvocation(uuid, i + 1);
            assertThat(stepInvocation).isNotNull();
            assertThat(stepInvocation.status()).isEqualTo(InvocationStatus.COMPLETE);
            assertThat(stepInvocation.attempts()).isEqualTo(1);
        }

        // Verify the 4th iteration (count=3) was started but not completed
        Invocation failedStep = executionLog.getInvocation(uuid, 4);
        assertThat(failedStep).isNotNull();
        assertThat(failedStep.status()).isEqualTo(InvocationStatus.PENDING);
        assertThat(failedStep.attempts()).isEqualTo(1);

        // Fix the failure condition and retry
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        flow.run(f -> f.sayHello());

        // Verify the failed step now has 2 attempts and is complete
        Invocation retriedStep = executionLog.getInvocation(uuid, 4);
        assertThat(retriedStep).isNotNull();
        assertThat(retriedStep.status()).isEqualTo(InvocationStatus.COMPLETE);
        assertThat(retriedStep.attempts()).isEqualTo(2);

        // Verify the 5th iteration (count=4) completed on first attempt
        Invocation lastStep = executionLog.getInvocation(uuid, 5);
        assertThat(lastStep).isNotNull();
        assertThat(lastStep.status()).isEqualTo(InvocationStatus.COMPLETE);
        assertThat(lastStep.attempts()).isEqualTo(1);
    }

    @Test
    public void shouldTrackMultipleAttempts() {
        HelloWorldFlow.FAIL_ON_COUNT = 2;
        UUID uuid = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);

        // First attempt - fails at count 2
        assertThatThrownBy(() -> flow.run(f -> f.sayHello()))
                .isInstanceOf(IllegalArgumentException.class);

        Invocation failedStep = executionLog.getInvocation(uuid, 3);
        assertThat(failedStep.attempts()).isEqualTo(1);

        // Second attempt - still fails
        assertThatThrownBy(() -> flow.run(f -> f.sayHello()))
                .isInstanceOf(IllegalArgumentException.class);

        failedStep = executionLog.getInvocation(uuid, 3);
        assertThat(failedStep.attempts()).isEqualTo(2);

        // Third attempt - still fails
        assertThatThrownBy(() -> flow.run(f -> f.sayHello()))
                .isInstanceOf(IllegalArgumentException.class);

        failedStep = executionLog.getInvocation(uuid, 3);
        assertThat(failedStep.attempts()).isEqualTo(3);

        // Fix and retry - should succeed
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        flow.run(f -> f.sayHello());

        failedStep = executionLog.getInvocation(uuid, 3);
        assertThat(failedStep.attempts()).isEqualTo(4);
        assertThat(failedStep.status()).isEqualTo(InvocationStatus.COMPLETE);
    }

    @Test
    public void shouldHandleMultipleFlowsIndependently() {
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow1 = Persistasaurus.getFlow(HelloWorldFlow.class, uuid1);
        FlowInstance<HelloWorldFlow> flow2 = Persistasaurus.getFlow(HelloWorldFlow.class, uuid2);

        flow1.run(f -> f.sayHello());
        flow2.run(f -> f.sayHello());

        // Verify both flows have independent logs
        Invocation flow1Invocation = executionLog.getInvocation(uuid1, 0);
        Invocation flow2Invocation = executionLog.getInvocation(uuid2, 0);

        assertThat(flow1Invocation).isNotNull();
        assertThat(flow2Invocation).isNotNull();
        assertThat(flow1Invocation.id()).isEqualTo(uuid1);
        assertThat(flow2Invocation.id()).isEqualTo(uuid2);
    }

    @Test
    public void shouldStoreMethodParameters() {
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        UUID uuid = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
        flow.run(f -> f.sayHello());

        // Verify parameters for second step call (count=1)
        Invocation stepInvocation = executionLog.getInvocation(uuid, 2);
        assertThat(stepInvocation).isNotNull();
        assertThat(stepInvocation.parameters()).isNotNull();
        assertThat(stepInvocation.parameters()).hasSize(2);
        assertThat(stepInvocation.parameters()[0]).isEqualTo("World");
        assertThat(stepInvocation.parameters()[1]).isEqualTo(1);
    }

    @Test
    public void shouldStoreReturnValues() {
        HelloWorldFlow.FAIL_ON_COUNT = -1;
        UUID uuid = UUID.randomUUID();

        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
        flow.run(f -> f.sayHello());

        // Verify return values for each step
        for (int i = 0; i < 5; i++) {
            Invocation stepInvocation = executionLog.getInvocation(uuid, i + 1);
            assertThat(stepInvocation.returnValue()).isEqualTo(i);
        }
    }
}
