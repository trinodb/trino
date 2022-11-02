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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPrecomputedHashes
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "true")
                .build();

        assertions = new QueryAssertions(session);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testDistinctLimit()
    {
        // see https://github.com/prestodb/presto/issues/11593
        assertThat(assertions.query(
                "SELECT a " +
                        "FROM (" +
                        "    SELECT a, b" +
                        "    FROM (VALUES (1, 2)) t(a, b)" +
                        "    WHERE a = 1" +
                        "    GROUP BY a, b" +
                        "    LIMIT 1" +
                        ")" +
                        "GROUP BY a"))
                .matches("VALUES (1)");
    }

    @Test
    public void testUnionAllAndDistinctLimit()
    {
        assertThat(assertions.query(
                "WITH t(a, b) AS (VALUES (1, 10))" +
                        "SELECT" +
                        "  count(DISTINCT if(type='A', a))," +
                        "  count(DISTINCT if(type='A', b))" +
                        "FROM (" +
                        "    SELECT a, b, 'A' AS type" +
                        "    FROM t" +
                        "    GROUP BY a, b" +
                        "    UNION ALL" +
                        "    SELECT a, b, 'B' AS type" +
                        "    FROM t" +
                        "    GROUP BY a, b)"))
                .matches("VALUES (BIGINT '1', BIGINT '1')");
    }
}
