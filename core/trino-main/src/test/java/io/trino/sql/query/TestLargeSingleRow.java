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

import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.EXCHANGE_COMPRESSION_CODEC;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLargeSingleRow
{
    @Test
    void testLargeRowThroughExchange()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(EXCHANGE_COMPRESSION_CODEC, "NONE")
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("local", "tpch");

        try (QueryAssertions assertions = new QueryAssertions(runner)) {
            assertThat(assertions.query(
                    """
                    WITH block_1mb AS (
                            -- Step 1: Create a 1 MB base block.
                            -- repeat is restricted to 100,000 repetitions * 10 bytes/char.
                            SELECT repeat(substr(comment, 1, 10), 100000) AS block_1m from lineitem LIMIT 1
                        ),
                        block_4mb AS (
                            -- Step 2: Create a 4 MB block by concatenating the 1 MB block four times.
                            SELECT
                                CONCAT(t1.block_1m, t1.block_1m, t1.block_1m, t1.block_1m) AS block_4m
                            FROM block_1mb t1
                        ),
                        final_payload AS (
                            -- Step 3: Concatenate the 4 MB block 12 times to get 48 MB.
                            SELECT
                                CONCAT(
                                    t2.block_4m, t2.block_4m, t2.block_4m, t2.block_4m,
                                    t2.block_4m, t2.block_4m, t2.block_4m, t2.block_4m,
                                    t2.block_4m, t2.block_4m, t2.block_4m, t2.block_4m
                                ) AS huge_comment
                            FROM block_4mb t2
                        )
                        SELECT
                            huge_comment[1]
                        FROM
                            final_payload
                        -- FINAL STEP: Select the huge column and ORDER BY, forcing a remote exchange
                        ORDER BY huge_comment LIMIT 1
                    """))
                    .succeeds();
        }
    }
}
