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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIssue16101
{
    @Test
    public void test()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner runner = LocalQueryRunner.builder(session).build();
        runner.createCatalog("local", new TpchConnectorFactory(1), ImmutableMap.of());

        try (QueryAssertions assertions = new QueryAssertions(runner)) {
            assertThat(assertions.query("""
                    SELECT orderkey, orderstatus, x
                    FROM (
                        SELECT orderkey, orderstatus, orderstatus = 'O' AS x
                        FROM orders) a
                    INNER JOIN ( VALUES 1, 2, 3, 4 ) b(k)
                    ON a.orderkey = b.k
                    WHERE orderstatus = 'O'
                    """))
                    .matches("""
                            VALUES
                                (BIGINT '1', 'O', true),
                                (BIGINT '2', 'O', true),
                                (BIGINT '4', 'O', true)
                            """);
        }
    }
}
