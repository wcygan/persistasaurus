# Persistasaurus

A minimal durable execution library for Java, based on SQLite.

Persistasaurus stores method invocations in a database. When restarting an invocation flow after a failure, e.g. a machine crash, it will resume from the last successful step of that flow, driving it to completion.

## Usage

Persistasaurus flows are identified by a UUID and are organized into steps.
Both flows and steps are arbitrary¹ Java methods.
Mark flows with the `@Flow` annotation and steps with `@Step`:

```
public class HelloWorldFlow {

   @Flow
   public void sayHello() {
     int sum = 0;

     for (int i = 0; i < 5; i++) {
       sum += say("World", i);
     }

     System.out.println(String.format("Sum: %s", sum));
   }

   @Step
   protected int say(String name, int count) {
     System.out.println(String.format("Hello, %s (%s)", name, count));
     return count;
  }
}
```

Retrieve and invoke a flow like so:

```
UUID uuid = UUID.randomUUID();
FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
flow.run(f -> f.sayHello());
```

If the program runs to completion, the following will be logged to stdout:

```
Hello, World (0)
Hello, World (1)
Hello, World (2)
Hello, World (3)
Hello, World (4)
Sum: 10
```

Now let's assume something goes wrong in the third steps before its result can be logged:

```
Hello, World (0)
Hello, World (1)
Hello, World (2)
IllegalStateException("Uh oh")
```

After restarting the flow (using the same UUID as before), it will retry that failed step and resume from there.
The first two steps which were already successfully are not re-executed:

```
Hello, World (3)
Hello, World (4)
Sum: 10
```

¹ Some restrictions apply: they must be non-final and have a visibility of `protected` or wider.

## Delayed Steps

Steps of a flow can be delayed.
To do so, add the `@Step::delay()` option to the step in question:

```
@Flow
public void sayHello() throws Exception {
   List<String> greetings = new ArrayList<>();
   greetings.add(greet("Bob"));

   String greeting = greetDelayed("Barry");
   greetings.add(greeting);

   System.out.println(String.format("All greetings: %s", greetings));
}

@Step
protected String greet(String name) {
    return String.format("Hello, %s", name);
}

@Step(delay = 5, timeUnit = ChronoUnit.SECONDS)
protected String greetDelayed(String name) {
    return String.format("Delayed hello, %s", name);
}
```

Run flows with delayed steps via `runAsync()`:

```
UUID uuid = UUID.randomUUID();
FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
flow.runAsync(f -> f.sayHello());
```

Under the hood, this will use a virtual thread for awaiting the given delay before proceeding with the workflow.
When aborting a flow while it is awaiting a delayed step and then restarting the flow, only any remainder of the delay (if any) will be awaited before executing the delayed step.

## Waiting for External Signals ("Human in the Loop")

For many flows it is required to wait for some external input before proceeding with a certain step.
This can be implemented using `await()` like so:

```
@Flow
public void signupUser(String name, String email) {
  createUserRecord(name, email);

  sendConfirmationEmail(name, email);

  await(() -> confirmEmailAddress(any()));

  sendWelcomeEmail(name, email);
}
```
This flow will wait after the `sendConfirmationEmail()` step, until the `confirmEmailAddress()` step has been triggered externally.
The parameter values passed to the awaiting step in the flow definition don't matter, you can pass `null` or the `any()` placeholder.

Later on, resume the flow like so, for instance from a Spring Boot REST method handler which receives email address confirmations:

```
@PostMapping("/email-confirmations")
void confirmEmailAddress(@RequestBody Confirmation confirmation) {
  FlowInstance<UserSignupFlow> flow = Persistasaurus.getFlow(UserSignupFlow.class, confirmation.uuid());

  flow.resume(f -> {
    f.confirmEmailAddress(confirmation.timestamp());
  });
}
```

The flow will now be resumed from the `confirmEmailAddress()` step, using the specified parameter value.

## Examining the Execution Log

You can query the execution log with any SQLite browser of your choice, for instance using DuckDB:

```
duckdb

ATTACH 'execution_log.db' AS db (TYPE SQLITE);
SELECT * FROM db.execution_log;
```

## Build

This project requires Java 25 or newer for building.
It comes with the Apache [Maven wrapper](https://maven.apache.org/tools/wrapper/),
i.e. a Maven distribution will be downloaded automatically, if needed.

Run the following command to build this project:

```shell
./mvnw clean verify
```

On Windows, run the following command:

```shell
mvnw.cmd clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```shell
./mvnw clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
./mvnw process-sources
```

## License

This code base is available under the Apache License, version 2.
