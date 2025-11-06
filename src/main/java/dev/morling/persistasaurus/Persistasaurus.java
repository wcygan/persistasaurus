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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.morling.persistasaurus.internal.ExecutionLog;
import dev.morling.persistasaurus.internal.ExecutionLog.Invocation;
import dev.morling.persistasaurus.internal.ExecutionLog.InvocationStatus;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatchers;

public class Persistasaurus {

    private static final Logger LOG = LoggerFactory.getLogger(Persistasaurus.class);

    private static final ExecutorService EXECUTOR;

    private static final ScopedValue<CallType> CALL_TYPE = ScopedValue.newInstance();

    private static final ConcurrentMap<UUID, WaitCondition> WAIT_CONDITIONS = new ConcurrentHashMap<>();

    static {
        EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
        recoverIncompleteFlows();
    }

    public static void recoverIncompleteFlows() {
        try {
            ExecutionLog executionLog = ExecutionLog.getInstance();
            List<Invocation> incompleteFlows = executionLog.getIncompleteFlows();

            if (!incompleteFlows.isEmpty()) {
                LOG.info("Found {} incomplete flows, scheduling for immediate execution", incompleteFlows.size());

                for (Invocation flow : incompleteFlows) {
                    LOG.info("Running incomplete flow {} for class {}.{} (attempt {})",
                            flow.id(), flow.className(), flow.methodName(), flow.attempts());

                    runFlowAsync(flow.id(), flow.className(), flow.methodName(), flow.parameters());
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

    private static void runFlowAsync(UUID id, String className, String methodName, Object[] parameters) {
        LOG.info("Running flow {} for class {}.{}", id, className, methodName);

        try {
            LOG.info("Executing delayed flow {} for class {}.{}",
                    id, className, methodName);

            Class<?> flowClass = Class.forName(className);
            FlowInstance<?> flow = getFlow(flowClass, id);
            flow.runAsync(c -> {
                Method method;
                try {
                    method = flowClass.getMethod(methodName, getParameterTypes(parameters));
                    method.invoke(c, parameters);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (Exception e) {
            LOG.error("Failed to execute delayed flow {} for class {}.{}: {}",
                    id, className, methodName, e.getMessage(), e);
        }
    }

    private static <T> T getFlowProxy(Class<T> clazz, UUID id) {
        try {
            return new ByteBuddy()
                    .subclass(clazz)
                    .method(ElementMatchers.any())
                    .intercept(
                            MethodDelegation.withDefaultConfiguration()
                                    .withBinders(Morph.Binder.install(OverrideCallable.class))
                                    .to(new Interceptor(id)))
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

    public static <T> FlowInstance<T> getFlow(Class<T> clazz, UUID uuid) {
        return new FlowInstance<>(getFlowProxy(clazz, uuid));
    }

    public static void await(Runnable r) {
        ScopedValue.where(CALL_TYPE, CallType.AWAIT).run(r);
    }

    public static class FlowInstance<T> {
        private T flow;

        public FlowInstance(T flow) {
            this.flow = flow;
        }

        public void run(Consumer<T> flowConsumer) {
            ScopedValue.where(CALL_TYPE, CallType.REGULAR).run(() -> flowConsumer.accept(flow));
        }

        public <R> R execute(Function<T, R> flowFunction) {
            return ScopedValue.where(CALL_TYPE, CallType.REGULAR).call(() -> flowFunction.apply(flow));
        }

        public void runAsync(Consumer<T> flowConsumer) {
            EXECUTOR.execute(() -> {
                ScopedValue.where(CALL_TYPE, CallType.REGULAR).run(() -> flowConsumer.accept(flow));
            });

        }

        public <R> CompletableFuture<R> executeAsync(Function<T, R> flowFunction) {
            return CompletableFuture.supplyAsync(() -> {
                return ScopedValue.where(CALL_TYPE, CallType.REGULAR).call(() -> flowFunction.apply(flow));
            },
                    EXECUTOR);
        }

        public void resume(Consumer<T> flowConsumer) {
            ScopedValue.where(CALL_TYPE, CallType.RESUME).run(() -> flowConsumer.accept(flow));
        }
    }

    public static class Interceptor {

        private static final Logger LOG = LoggerFactory.getLogger(Interceptor.class);

        private final ExecutionLog executionLog;
        private final UUID id;
        private int step;

        public Interceptor(UUID id) {
            this.executionLog = ExecutionLog.getInstance();
            this.id = id;
            this.step = 0;
        }

        @RuntimeType
        public Object intercept(@This Object instance,
                                @Origin Method method,
                                @AllArguments Object[] args,
                                @Morph OverrideCallable callable)
                throws Throwable {

            if (!requiresLogging(method)) {
                return callable.call(args);
            }

            String className = method.getDeclaringClass().getName();
            String methodName = method.getName();
            Duration delay = getDelay(method);
            CallType callType = CALL_TYPE.get();

            if (isFlow(method)) {
                step = 0;
                LOG.info("Starting flow: {}.{}", className, methodName);
            }

            Invocation loggedInvocation = null;

            if (callType == CallType.RESUME) {
                loggedInvocation = executionLog.getLatestInvocation(id);
                step = loggedInvocation.step();
            }
            else {
                loggedInvocation = executionLog.getInvocation(id, step);
            }

            Duration remainingDelay = delay;

            if (loggedInvocation != null) {
                if (!loggedInvocation.className().equals(className) || !loggedInvocation.methodName().equals(methodName)) {
                    throw new IllegalStateException("Incompatible change of flow structure");
                }

                if (loggedInvocation.status() == InvocationStatus.COMPLETE) {
                    LOG.info("Replaying completed step {}: {}.{} with args {} -> {}",
                            step, className, methodName, Arrays.toString(args), loggedInvocation.returnValue());
                    step++;

                    return loggedInvocation.returnValue();
                }
                else if (loggedInvocation.status() == InvocationStatus.WAITING_FOR_SIGNAL && callType == CallType.RESUME) {
                    LOG.info("Resuming waiting step {}: {}.{}", step, className, methodName);
                    WaitCondition waitCondition = WAIT_CONDITIONS.get(id);

                    waitCondition.lock.lock();

                    try {
                        WAIT_CONDITIONS.put(id, waitCondition.withResumeParameterValues(args));
                        waitCondition.condition.signal();
                    }
                    finally {
                        waitCondition.lock.unlock();
                    }

                    return null;
                }
                else {
                    LOG.info("Retrying incomplete step {} (attempt {}): {}.{} with args {}",
                            step, loggedInvocation.attempts() + 1, className, methodName, Arrays.toString(args));

                    if (delay != null) {
                        remainingDelay = Instant.now().until(loggedInvocation.timestamp().plus(delay));
                    }
                }
            }

            // Log invocation start (or increment attempts on retry via ON CONFLICT)
            executionLog.logInvocationStart(id, step, className, methodName, delay,
                    callType == CallType.AWAIT ? InvocationStatus.WAITING_FOR_SIGNAL : InvocationStatus.PENDING, args);

            if (delay != null && remainingDelay.isPositive()) {
                if (!Thread.currentThread().isVirtual()) {
                    throw new IllegalStateException(
                            "Can't run a delayed step on a non-virtual thread. Make sure to execute flows with delayed steps via FlowInstance::runAsync().");
                }

                LOG.info("Delaying step {}: {}.{} with args {} for {}", step, className, methodName, Arrays.toString(args), remainingDelay);
                Thread.sleep(remainingDelay);
            }
            else if (callType == CallType.AWAIT) {
                if (!Thread.currentThread().isVirtual()) {
                    throw new IllegalStateException(
                            "Can't run a step waiting for an external signal on a non-virtual thread. Make sure to execute flows with wait steps via FlowInstance::runAsync().");
                }

                WaitCondition waitCondition = WAIT_CONDITIONS.computeIfAbsent(id, id -> {
                    ReentrantLock l = new ReentrantLock();
                    return new WaitCondition(l, l.newCondition(), null);
                });

                waitCondition.lock.lock();

                try {
                    LOG.info("Awaiting step {}: {}.{}", step, className, methodName);
                    waitCondition.condition.await();
                    args = WAIT_CONDITIONS.get(id).resumeParameterValues();
                }
                finally {
                    waitCondition.lock.unlock();
                }
            }

            LOG.info("Executing step {}: {}.{} with args {}", step, className, methodName, Arrays.toString(args));

            int currentStep = step;
            step++;
            Object result = callable.call(args);

            executionLog.logInvocationCompletion(id, currentStep, result);
            LOG.info("Completed step {}: {}.{} -> {}", currentStep, className, methodName, result);

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

    public static interface OverrideCallable {
        Object call(Object[] args) throws Throwable;
    }

    private static record WaitCondition(ReentrantLock lock, Condition condition, Object[] resumeParameterValues) {

        WaitCondition withResumeParameterValues(Object[] resumeParameterValues) {
            return new WaitCondition(lock, condition, resumeParameterValues);
        }
    }

    private enum CallType {
        REGULAR,
        AWAIT,
        RESUME;
    }
}
