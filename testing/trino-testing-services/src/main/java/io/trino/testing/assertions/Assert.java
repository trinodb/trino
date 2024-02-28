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
package io.trino.testing.assertions;

import io.airlift.units.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Assert
{
    private Assert() {}

    public static <E extends Exception> void assertEventually(CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(new Duration(30, SECONDS), assertion);
    }

    public static <E extends Exception> void assertEventually(Duration timeout, CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(timeout, new Duration(50, MILLISECONDS), assertion);
    }

    public static <E extends Exception> void assertEventually(Duration timeout, Duration retryFrequency, CheckedRunnable<E> assertion)
            throws E
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                assertion.run();
                return;
            }
            catch (Exception | AssertionError e) {
                if (Duration.nanosSince(start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            try {
                Thread.sleep(retryFrequency.toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public interface CheckedRunnable<E extends Exception>
    {
        void run()
                throws E;
    }
}
