/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.Invocation;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatchers;

public class Persistasaurus {

    private static final Logger LOG = LoggerFactory.getLogger(Persistasaurus.class);

    private final ScheduledExecutorService scheduler;

    public Persistasaurus() {
        scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
    }

    public void recoverIncompleteFlows() {
        try {
            ExecutionLog executionLog = ExecutionLog.getInstance();
            List<Invocation> incompleteFlows = executionLog.getIncompleteFlows();

            if (!incompleteFlows.isEmpty()) {
                LOG.info("Found {} incomplete flows, scheduling for immediate execution", incompleteFlows.size());

                for (Invocation flow : incompleteFlows) {
                    LOG.info("Scheduling incomplete flow {} for class {}.{} (attempt {})",
                            flow.id(), flow.className(), flow.methodName(), flow.attempts());

                    scheduleDelayedFlow(flow.id(), flow.className(), flow.methodName(),
                            flow.parameters(), Duration.ZERO);
                }
            }
        }
        catch (Exception e) {
            LOG.error("Failed to schedule incomplete flows: {}", e.getMessage(), e);
        }
    }

    private static Class<?>[] getParameterTypes(Object[] parameters) {
        if (parameters == null || parameters.length == 0) {
            return new Class<?>[0];
        }
        return Arrays.stream(parameters)
                .map(obj -> obj == null ? Object.class : obj.getClass())
                .toArray(Class<?>[]::new);
    }

    private void scheduleDelayedFlow(UUID id, String className, String methodName, Object[] parameters, Duration delay) {
        LOG.info("Scheduling delayed flow {} for class {}.{} with delay of {} seconds",
                id, className, methodName, delay.getSeconds());

        scheduler.schedule(() -> {
            try {
                LOG.info("Executing delayed flow {} for class {}.{}",
                        id, className, methodName);

                Class<?> flowClass = Class.forName(className);
                Object flow = getFlow(flowClass, id);

                Method method = flowClass.getMethod(methodName, getParameterTypes(parameters));
                method.invoke(flow, parameters);
            }
            catch (Exception e) {
                LOG.error("Failed to execute delayed flow {} for class {}.{}: {}",
                        id, className, methodName, e.getMessage(), e);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public <T> T getFlow(Class<T> clazz, UUID id) {
        try {
            return new ByteBuddy()
                    .subclass(clazz)
                    .method(ElementMatchers.any())
                    .intercept(MethodDelegation.to(new Interceptor(this, id)))
                    .make()
                    .load(Persistasaurus.class.getClassLoader())
                    .getLoaded()
                    .getDeclaredConstructor()
                    .newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't instantiate flow", e);
        }
    }

    public static class Interceptor {

        private static final Logger LOG = LoggerFactory.getLogger(Interceptor.class);

        private final ExecutionLog executionLog;
        private final Persistasaurus persistasaurus;
        private final UUID id;
        private int step;
        private boolean delayed;

        public Interceptor(Persistasaurus persistasaurus, UUID id) {
            this.executionLog = ExecutionLog.getInstance();
            this.persistasaurus = persistasaurus;
            this.id = id;
            this.step = 0;
            this.delayed = false;
        }

        @RuntimeType
        public Object intercept(@This Object instance,
                                @Origin Method method,
                                @AllArguments Object[] args,
                                @SuperCall Callable<?> zuper)
                throws Exception {

            if (!requiresLogging(method)) {
                return zuper.call();
            }

            String className = method.getDeclaringClass().getName();
            String methodName = method.getName();
            Duration delay = getDelay(method);

            if (isFlow(method)) {
                step = 0;
                LOG.info("Starting flow: {}.{}", className, methodName);
            }

            Invocation loggedInvocation = executionLog.getInvocation(id, step);
            boolean isDelayElapsed = false;

            if (loggedInvocation != null) {
                if (!loggedInvocation.className().equals(className) || !loggedInvocation.methodName().equals(methodName)) {
                    throw new IllegalStateException("Incompatible change of flow structure");
                }

                if (loggedInvocation.isComplete()) {
                    LOG.info("Replaying completed step {}: {}.{} with args {} -> {}",
                            step, className, methodName, Arrays.toString(args), loggedInvocation.returnValue());
                    step++;

                    if (method.getReturnType() == CompletableFuture.class) {
                        return CompletableFuture.completedFuture(loggedInvocation.returnValue());
                    }
                    else {
                        return loggedInvocation.returnValue();
                    }

                }
                else {
                    LOG.info("Retrying incomplete step {} (attempt {}): {}.{} with args {}",
                            step, loggedInvocation.attempts() + 1, className, methodName, Arrays.toString(args));

                    if (delay != null) {
                        isDelayElapsed = loggedInvocation.timestamp().plus(delay).isBefore(Instant.now());
                    }
                }
            }

            // Log invocation start (or increment attempts on retry via ON CONFLICT)
            executionLog.logInvocationStart(id, step, className, methodName, delay, args);

            if (delay != null && !isDelayElapsed) {
                LOG.info("Delaying step {}: {}.{} with args {}", step, className, methodName, Arrays.toString(args));
                delayed = true;

                // If this is the first time encountering this delayed step, schedule the flow for execution
                if (loggedInvocation == null) {
                    // Get the flow invocation (step 0) to schedule it
                    Invocation flowInvocation = executionLog.getInvocation(id, 0);
                    persistasaurus.scheduleDelayedFlow(id, flowInvocation.className(), flowInvocation.methodName(),
                            flowInvocation.parameters(), delay);
                }

                return new CompletableFuture<>();
            }

            LOG.info("Executing step {}: {}.{} with args {}", step, className, methodName, Arrays.toString(args));

            int currentStep = step;
            step++;
            Object result = zuper.call();

            if (!delayed) {
                executionLog.logInvocationCompletion(id, currentStep, result);
                LOG.info("Completed step {}: {}.{} -> {}", currentStep, className, methodName, result);
            }

            return result;
        }

        private Duration getDelay(Method method) {
            if (!method.isAnnotationPresent(Step.class)) {
                return null;
            }

            long delay = method.getAnnotation(Step.class).delay();
            ChronoUnit unit = method.getAnnotation(Step.class).timeUnit();

            return delay != Long.MIN_VALUE ? Duration.of(delay, unit) : null;
        }

        private boolean isFlow(Method method) {
            return method.isAnnotationPresent(Flow.class);
        }

        private boolean requiresLogging(Method method) {
            return isFlow(method) || method.isAnnotationPresent(Step.class);
        }
    }
}
