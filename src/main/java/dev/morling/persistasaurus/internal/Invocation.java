/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus.internal;

import java.time.Instant;
import java.util.UUID;

import dev.morling.persistasaurus.internal.ExecutionLog.InvocationStatus;

public record Invocation(
                         UUID id,
                         int step,
                         Instant timestamp,
                         String className,
                         String methodName,
                         InvocationStatus status,
                         int attempts,
                         Object[] parameters,
                         Object returnValue) {

    public boolean isFlow() {
        return step == 0;
    }
}