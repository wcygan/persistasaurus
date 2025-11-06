/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

import java.util.UUID;

public class FlowTestApp {

    public static class HelloWorldFlow {

        public static int FAIL_ON_COUNT = -1;

        @Flow
        public void sayHello() {
            say("World");
        }

        @Step(delay = 60)
        protected void say(String name) {
            System.out.println(String.format("Hello, %s", name));
        }
    }

    public static void main(String[] args) throws Exception {
        // ExecutionLog executionLog = ExecutionLog.getInstance();
        // executionLog.reset();

        // UUID uuid = UUID.randomUUID();
        UUID uuid = UUID.fromString("bcd2f9e4-4ffa-46b4-b797-7cb486020eea");
        System.out.println(uuid);
        FlowInstance<HelloWorldFlow> flow = Persistasaurus.getFlow(HelloWorldFlow.class, uuid);
        flow.runAsync(f -> f.sayHello());

        Thread.sleep(7000);
    }
}
