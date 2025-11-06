/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import dev.morling.persistasaurus.Persistasaurus.CallType;

/**
 * One execution of a flow.
 *
 * @param <T> The flow type.
 */
public class FlowInstance<T> {

    private final UUID id;
    private final T flow;

    public FlowInstance(UUID id, T flow) {
        this.id = id;
        this.flow = flow;
    }

    public void run(Consumer<T> flowConsumer) {
        ScopedValue.where(Persistasaurus.CALL_TYPE, CallType.RUN).run(() -> flowConsumer.accept(flow));
    }

    public <R> R execute(Function<T, R> flowFunction) {
        return ScopedValue.where(Persistasaurus.CALL_TYPE, CallType.RUN).call(() -> flowFunction.apply(flow));
    }

    public void runAsync(Consumer<T> flowConsumer) {
        Persistasaurus.EXECUTOR.execute(() -> {
            ScopedValue.where(Persistasaurus.CALL_TYPE, CallType.RUN).run(() -> flowConsumer.accept(flow));
        });

    }

    public <R> CompletableFuture<R> executeAsync(Function<T, R> flowFunction) {
        return CompletableFuture.supplyAsync(() -> {
            return ScopedValue.where(Persistasaurus.CALL_TYPE, CallType.RUN).call(() -> flowFunction.apply(flow));
        },
                Persistasaurus.EXECUTOR);
    }

    public void resume(Consumer<T> flowConsumer) {
        ScopedValue.where(Persistasaurus.CALL_TYPE, CallType.RESUME).run(() -> flowConsumer.accept(flow));
    }

    public UUID id() {
        return id;
    }
}
