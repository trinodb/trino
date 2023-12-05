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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIssue16101
{
    @Test
    public void test()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("local", "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));

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
