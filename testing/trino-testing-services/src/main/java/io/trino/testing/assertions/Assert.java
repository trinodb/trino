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

import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.airlift.units.Duration;

import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Assert
{
    private static final int MAX_REPORTED_ELEMENTS = 100;

    private Assert() {}

    /**
     * Asserts that {@code actual} and {@code expected} contain the same elements with the same multiplicities, in any order.
     * Unlike AssertJ's {@code containsExactlyInAnyOrder}, which is quadratic, this is linear in the element count.
     */
    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertEqualsIgnoreOrder(actual, expected, "Elements");
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected, String subject)
    {
        Multiset<?> actualMultiset = LinkedHashMultiset.create(actual);
        Multiset<?> expectedMultiset = LinkedHashMultiset.create(expected);
        if (actualMultiset.equals(expectedMultiset)) {
            return;
        }

        Multiset<?> unexpected = Multisets.difference(actualMultiset, expectedMultiset);
        Multiset<?> missing = Multisets.difference(expectedMultiset, actualMultiset);
        StringBuilder message = new StringBuilder("%s do not match: %s unexpected, %s missing (actual count: %s, expected count: %s)".formatted(
                subject,
                unexpected.size(),
                missing.size(),
                actualMultiset.size(),
                expectedMultiset.size()));
        appendElements(message, "Unexpected", unexpected);
        appendElements(message, "Missing", missing);
        throw new AssertionError(message.toString());
    }

    /**
     * Asserts that every element of {@code expected} occurs in {@code actual}, ignoring multiplicities.
     * Unlike AssertJ's {@code containsAll}, which is quadratic, this is linear in the element count.
     */
    public static void assertContainsAll(Iterable<?> actual, Iterable<?> expected)
    {
        assertContainsAll(actual, expected, "Elements");
    }

    public static void assertContainsAll(Iterable<?> actual, Iterable<?> expected, String subject)
    {
        Multiset<?> actualMultiset = LinkedHashMultiset.create(actual);
        Set<?> actualElements = actualMultiset.elementSet();
        Multiset<Object> missing = LinkedHashMultiset.create();
        int expectedCount = 0;
        for (Object element : expected) {
            expectedCount++;
            if (!actualElements.contains(element)) {
                missing.add(element);
            }
        }
        if (missing.isEmpty()) {
            return;
        }

        StringBuilder message = new StringBuilder("%s do not contain all expected: %s missing (actual count: %s, expected count: %s)".formatted(
                subject,
                missing.size(),
                actualMultiset.size(),
                expectedCount));
        appendElements(message, "Missing", missing);
        appendElements(message, "Actual", actualMultiset);
        throw new AssertionError(message.toString());
    }

    private static void appendElements(StringBuilder message, String title, Multiset<?> elements)
    {
        if (elements.isEmpty()) {
            return;
        }
        int distinctCount = elements.entrySet().size();
        message.append("\n").append(title);
        if (distinctCount > MAX_REPORTED_ELEMENTS) {
            message.append(" (first %s of %s distinct)".formatted(MAX_REPORTED_ELEMENTS, distinctCount));
        }
        message.append(":");
        for (Multiset.Entry<?> entry : Iterables.limit(elements.entrySet(), MAX_REPORTED_ELEMENTS)) {
            message.append("\n  ").append(entry.getElement());
            if (entry.getCount() > 1) {
                message.append(" (%s times)".formatted(entry.getCount()));
            }
        }
    }

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

    public static <E extends Exception> void assertConsistently(Duration timeout, Duration retryFrequency, Assert.CheckedRunnable<E> assertion)
            throws E
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            assertion.run();

            if (Duration.nanosSince(start).compareTo(timeout) > 0) {
                return;
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
