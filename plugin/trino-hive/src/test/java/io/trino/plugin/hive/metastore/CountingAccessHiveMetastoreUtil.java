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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.String.join;
import static org.testng.Assert.fail;

public final class CountingAccessHiveMetastoreUtil
{
    private CountingAccessHiveMetastoreUtil() {}

    public static void assertMetastoreInvocations(
            CountingAccessHiveMetastore metastore,
            QueryRunner queryRunner,
            Session session,
            @Language("SQL") String query,
            Multiset<?> expectedInvocations)
    {
        metastore.resetCounters();
        queryRunner.execute(session, query);
        Multiset<CountingAccessHiveMetastore.Method> actualInvocations = metastore.getMethodInvocations();

        if (expectedInvocations.equals(actualInvocations)) {
            return;
        }

        List<String> mismatchReport = Sets.union(expectedInvocations.elementSet(), actualInvocations.elementSet()).stream()
                .filter(key -> expectedInvocations.count(key) != actualInvocations.count(key))
                .flatMap(key -> {
                    int expectedCount = expectedInvocations.count(key);
                    int actualCount = actualInvocations.count(key);
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
