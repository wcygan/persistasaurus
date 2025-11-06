/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import static dev.morling.persistasaurus.Persistasaurus.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.persistasaurus.Persistasaurus.FlowInstance;
import dev.morling.persistasaurus.internal.ExecutionLog;

public class ExternallyResumedFlowTest {

    private ExecutionLog executionLog;

    @BeforeEach
    public void setup() {
        executionLog = ExecutionLog.getInstance();
        executionLog.reset();

        // Reset static latches
        SignupFlow.SEND_CONFIRMATION_INVOKED = new CountDownLatch(1);
        SignupFlow.CONFIRM_INVOKED = new CountDownLatch(1);
        SignupFlow.SEND_WELCOME_INVOKED = new CountDownLatch(1);
        SignupFlow.CONFIRMATION_TIMESTAMP = null;
    }

    @Test
    public void shouldRunFlowWithResumeStep() throws InterruptedException {
        UUID uuid = UUID.randomUUID();
        FlowInstance<SignupFlow> flow = Persistasaurus.getFlow(SignupFlow.class, uuid);

        flow.runAsync(f -> f.signupUser("Bob", "bob@example.com"));
        SignupFlow.SEND_CONFIRMATION_INVOKED.await();
        assertThat(SignupFlow.CONFIRM_INVOKED.getCount()).isNotEqualTo(0);

        Instant confirmationTimestamp = Instant.parse("2025-11-03T10:30:00Z");
        flow.resume(f -> {
            f.confirmEmail(confirmationTimestamp);
        });

        SignupFlow.SEND_WELCOME_INVOKED.await();

        assertThat(SignupFlow.CONFIRMATION_TIMESTAMP).isEqualTo(confirmationTimestamp);
    }

    @Test
    public void shouldFailWhenResumingFromWrongStep() throws InterruptedException {
        UUID uuid = UUID.randomUUID();
        FlowInstance<SignupFlow> flow = Persistasaurus.getFlow(SignupFlow.class, uuid);

        flow.runAsync(f -> f.signupUser("Bob", "bob@example.com"));
        SignupFlow.SEND_CONFIRMATION_INVOKED.await();
        assertThat(SignupFlow.CONFIRM_INVOKED.getCount()).isNotEqualTo(0);

        try {
            // Try to resume from a different step than the one that's waiting
            flow.resume(r -> {
                r.sendWelcomeEmail("Bob", "bob@example.com");
            });

            fail("Expected exception wasn't raised");

        }
        catch (IllegalStateException e) {
            assertThat(e).hasMessage("Incompatible change of flow structure");
        }
    }

    private static <T> T any() {
        return null;
    }

    public static class SignupFlow {

        public static volatile Instant CONFIRMATION_TIMESTAMP;

        private static CountDownLatch SEND_CONFIRMATION_INVOKED = new CountDownLatch(1);
        private static CountDownLatch CONFIRM_INVOKED = new CountDownLatch(1);
        private static CountDownLatch SEND_WELCOME_INVOKED = new CountDownLatch(1);

        @Flow
        public void signupUser(String name, String email) {
            createUserRecord(name, email);

            sendConfirmationEmail(name, email);

            await(() -> confirmEmail(any()));

            sendWelcomeEmail(name, email);
        }

        @Step
        protected void createUserRecord(String name, String email) {
            System.out.println(String.format("Creating record for user %s (%s)", name, email));
        }

        @Step
        protected void sendConfirmationEmail(String name, String email) {
            System.out.println(String.format("Sending confirmation email to user %s (%s)", name, email));
            SEND_CONFIRMATION_INVOKED.countDown();
        }

        @Step
        protected void confirmEmail(Instant timeOfConfirmation) {
            System.out.println("Email confirmed at " + timeOfConfirmation);
            CONFIRMATION_TIMESTAMP = timeOfConfirmation;

            if (CONFIRM_INVOKED.getCount() == 0) {
                throw new IllegalStateException("Invoked before");
            }
            CONFIRM_INVOKED.countDown();
        }

        @Step
        protected void sendWelcomeEmail(String name, String email) {
            System.out.println(String.format("Sending welcome email to user %s (%s)", name, email));
            SEND_WELCOME_INVOKED.countDown();
        }
    }
}
