/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;

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

    public static <T> T getFlow(Class<T> clazz, UUID id) {
        try {
            return new ByteBuddy()
                    .subclass(clazz)
                    .method(ElementMatchers.any())
                    .intercept(MethodDelegation.to(new Interceptor(id)))
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
        private final UUID id;
        private int step;

        public Interceptor(UUID id) {
            this.executionLog = new ExecutionLog();
            this.id = id;
            this.step = 0;
        }

        @RuntimeType
        public Object intercept(@This Object instance,
                                @Origin Method method,
                                @AllArguments Object[] args,
                                @SuperCall Callable<?> zuper)
                throws Exception {

            String className = method.getDeclaringClass().getName();
            String methodName = method.getName();

            if (!requiresLogging(method)) {
                return zuper.call();
            }

            if (isFlow(method)) {
                step = 0;
                LOG.info("Starting flow: {}.{}", className, methodName);
            }

            Invocation loggedInvocation = executionLog.getInvocation(id, step);
            if (loggedInvocation != null) {
                if (!loggedInvocation.className().equals(loggedInvocation.className()) || !loggedInvocation.methodName().equals(methodName)) {
                    throw new IllegalStateException("Incompatible change of flow structure");
                }

                if (loggedInvocation.isComplete()) {
                    LOG.info("Replaying completed step {}: {}.{} with args {} -> {}",
                            step, className, methodName, Arrays.toString(args), loggedInvocation.returnValue());
                    step++;
                    return loggedInvocation.returnValue();
                }
                else {
                    LOG.info("Retrying incomplete step {} (attempt {}): {}.{} with args {}",
                            step, loggedInvocation.attempts() + 1, className, methodName, Arrays.toString(args));
                }
            }

            LOG.info("Executing step {}: {}.{} with args {}", step, className, methodName, Arrays.toString(args));

            // Log invocation start (or increment attempts on retry via ON CONFLICT)
            executionLog.logInvocationStart(id, step, className, methodName, args);

            int currentStep = step;
            step++;
            Object result = zuper.call();
            executionLog.logInvocationCompletion(id, currentStep, result);

            LOG.info("Completed step {}: {}.{} -> {}", currentStep, className, methodName, result);

            return result;
        }

        private boolean isFlow(Method method) {
            return method.isAnnotationPresent(Flow.class);
        }

        private boolean requiresLogging(Method method) {
            return isFlow(method) || method.isAnnotationPresent(Step.class);
        }
    }
}
