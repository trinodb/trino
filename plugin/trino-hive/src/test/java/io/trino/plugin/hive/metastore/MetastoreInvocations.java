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
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;

public final class MetastoreInvocations
{
    private static final String TRACE_PREFIX = "HiveMetastore.";

    private MetastoreInvocations() {}

    public static void assertMetastoreInvocationsForQuery(
            QueryRunner queryRunner,
            Session session,
            @Language("SQL") String query,
            Multiset<MetastoreMethod> expectedInvocations)
    {
        queryRunner.execute(session, query);

        Multiset<MetastoreMethod> invocations = queryRunner.getSpans().stream()
                .map(SpanData::getName)
                .filter(name -> name.startsWith(TRACE_PREFIX))
                .map(name -> name.substring(TRACE_PREFIX.length()))
                .filter(name -> !name.equals("listRoleGrants"))
                .filter(name -> !name.equals("listTablePrivileges"))
                .map(MetastoreMethod::fromMethodName)
                .collect(toImmutableMultiset());

        assertMultisetsEqual(invocations, expectedInvocations);
    }
}
