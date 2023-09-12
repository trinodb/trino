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
import io.trino.Session;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;

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
        assertMultisetsEqual(metastore.getMethodInvocations(), expectedInvocations);
    }
}
