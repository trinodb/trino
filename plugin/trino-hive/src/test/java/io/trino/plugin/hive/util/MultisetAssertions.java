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
package io.trino.plugin.hive.util;

import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.String.join;
import static org.testng.Assert.fail;

public final class MultisetAssertions
{
    private MultisetAssertions() {}

    public static void assertMultisetsEqual(Multiset<?> actual, Multiset<?> expected)
    {
        if (expected.equals(actual)) {
            return;
        }

        List<String> mismatchReport = Sets.union(expected.elementSet(), actual.elementSet()).stream()
                .filter(key -> expected.count(key) != actual.count(key))
                .flatMap(key -> {
                    int expectedCount = expected.count(key);
                    int actualCount = actual.count(key);
                    if (actualCount < expectedCount) {
                        return Stream.of(format("%s more occurrences of %s", expectedCount - actualCount, key));
                    }
                    if (actualCount > expectedCount) {
                        return Stream.of(format("%s fewer occurrences of %s", actualCount - expectedCount, key));
                    }
                    return Stream.of();
                })
                .collect(toImmutableList());

        fail("Expected: \n\t\t" + join(",\n\t\t", mismatchReport));
    }
}
