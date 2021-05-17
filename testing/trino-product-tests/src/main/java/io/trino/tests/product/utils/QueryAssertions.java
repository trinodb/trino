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
package io.trino.tests.product.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;
import io.trino.tempto.query.QueryResult;

import java.util.function.Supplier;

import static java.lang.String.format;
import static org.testng.Assert.fail;

public final class QueryAssertions
{
    public static void assertContainsEventually(Supplier<QueryResult> all, QueryResult expectedSubset, Duration timeout)
    {
        assertEventually(timeout, () -> assertContains(all.get(), expectedSubset));
    }

    public static void assertEventually(Duration timeout, Runnable assertion)
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
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public static void assertContains(QueryResult all, QueryResult expectedSubset)
    {
        for (Object row : expectedSubset.rows()) {
            if (!all.rows().contains(row)) {
                fail(format("expected row missing: %s\nAll %s rows:\n    %s\nExpected subset %s rows:\n    %s\n",
                        row,
                        all.getRowsCount(),
                        Joiner.on("\n    ").join(Iterables.limit(all.rows(), 100)),
                        expectedSubset.getRowsCount(),
                        Joiner.on("\n    ").join(Iterables.limit(expectedSubset.rows(), 100))));
            }
        }
    }

    private QueryAssertions() {}
}
