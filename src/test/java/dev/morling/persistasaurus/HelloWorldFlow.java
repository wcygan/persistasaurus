/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.persistasaurus;

public class HelloWorldFlow {

    public static int FAIL_ON_COUNT = -1;

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
        if (count == FAIL_ON_COUNT) {
            throw new IllegalArgumentException(String.format("I don't like this count: %s", count));
        }

        System.out.println(String.format("Hello, %s (%s)", name, count));

        return count;
    }
}
