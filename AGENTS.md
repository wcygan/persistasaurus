# Project: Persistasaurus

## Overview

Persistasaurus is a Proof-of-Concept (PoC) Durable Execution (DE) engine implemented in Java. It allows developers to define multi-step workflows as regular Java code, ensuring that if a flow is interrupted (e.g., system crash), it can resume from the last successfully executed step without repeating side effects.

## Technology Stack

-   **Language**: Java 21+ (Required for Virtual Threads and Scoped Values).
-   **Persistence**: SQLite (Embedded, WAL mode).
-   **Bytecode Manipulation**: ByteBuddy (for creating dynamic proxies).
-   **Concurrency**: Virtual Threads (Project Loom) and Structured Concurrency.

## Core Concepts & Abstractions

### 1. Workflows (`@Flow`)
-   The entry point of a business process.
-   Defined as a method within a class annotated with `@Flow`.
-   Orchestrates the execution of Steps.

### 2. Steps (`@Step`)
-   The atomic unit of persistence and retriability.
-   Annotated with `@Step`.
-   **Behavior**:
    -   **Pending**: Logged before execution starts.
    -   **Complete**: Logged after successful execution with return value.
    -   **Replay**: If a step is already `COMPLETE` in the log, the method is **not** executed again. The stored return value is returned immediately.
    -   **Retry**: If a step is `PENDING` (failed/crashed previously), it is re-executed.

### 3. The Execution Log
-   A local SQLite database (`execution_log.db`) acting as a Write-Ahead Log.
-   Stores `Invocation` records: `flowId`, `step`, `status`, `parameters`, and `returnValue`.

### 4. Virtual Threads & Concurrency
-   The engine relies heavily on **Virtual Threads**.
-   **Delays**: `@Step(delay=...)` puts the virtual thread to sleep, unmounting it from the carrier thread.
-   **Signals**: `Persistasaurus.await()` suspends the virtual thread using `ReentrantLock` until an external signal is received via `resume()`.

### 5. Scoped Values
-   Uses `ScopedValue<CallType>` to implicitly pass execution context down the stack.
-   **Contexts**:
    -   `RUN`: Normal execution.
    -   `AWAIT`: The flow is suspending to wait for a signal.
    -   `RESUME`: The flow is being resumed with external data.

## Implementation Details

### Proxies
The engine does not require the user to call specific API methods for steps. Instead, `Persistasaurus.getFlow(Class, UUID)` returns a ByteBuddy proxy. This proxy intercepts all method calls to handle logging, persistence, replay, and suspension.

### Recovery
On startup, `Persistasaurus` automatically scans the `ExecutionLog` for incomplete flows and submits them to the executor for resumption.

## Coding Guidelines

1.  **Defining Flows**:
    -   Keep `@Flow` methods focused on orchestration.
    -   Move all side-effects (IO, DB calls, API requests) into `@Step` methods.
    -   Ensure arguments and return values of `@Step` methods are `Serializable`.

2.  **Executing Flows**:
    -   Use `Persistasaurus.getFlow(Class, UUID)` to obtain a flow instance.
    -   Prefer `flow.runAsync()` or `flow.executeAsync()` to run on Virtual Threads, especially if the flow contains delays or awaits signals.
    -   Use `flow.resume()` to provide data to a step waiting within an `await()` block.

3.  **Determinism**:
    -   Flow logic outside of `@Step` methods must be deterministic (avoid `Random`, `Instant.now()` outside steps) because it will be re-executed during replay. Capture non-deterministic values inside `@Step` methods.

## Example Usage

```java
@Flow
public void myWorkflow() {
    stepOne();
    stepTwo();
}

@Step
protected void stepOne() { ... }

@Step
protected void stepTwo() { ... }
```
