/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.testing.services.junit;

import com.google.errorprone.annotations.FormatMethod;

import static java.lang.String.format;

/**
 * @see io.trino.testng.services.Listeners for utlity class for TestNG listeners
 */
public final class Listeners
{
    private Listeners() {}

    /**
     * Print error to standard error and exit JVM.
     *
     * @apiNote A JUnit listener cannot throw an exception, as they are not propagated by JUnit framework.
     */
    @FormatMethod
    public static void reportListenerFailure(Class<?> listenerClass, String format, Object... args)
    {
        System.err.println(format("FATAL: %s: ", listenerClass.getName()) + format(format, args));
        System.err.println("JVM will be terminated");

        // JUnit does not fail on a listener exception.
        // Therefore, instead of throwing, we terminate the JVM.
        System.exit(1);
    }
}
