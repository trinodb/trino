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

import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.SystemSessionProperties.USE_EXACT_PARTITIONING;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIssue32078
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions(testSessionBuilder()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .setSystemProperty(USE_EXACT_PARTITIONING, "true")
                .build())) {
            // with right joins
            assertThat(assertions.query(
                    """
                    SELECT count_values FROM
                           (SELECT BIGINT '0' AS corr FROM (VALUES BIGINT '0') l(k) RIGHT JOIN (VALUES BIGINT '0', BIGINT '1') r(k) ON l.k = r.k) t
                           CROSS JOIN LATERAL (
                           SELECT count(*) AS count_values
                           FROM (VALUES BIGINT '1', BIGINT '2') u(v)
                           WHERE v > corr)
                    """))
                    .matches("VALUES BIGINT '2', BIGINT '2'");

            assertThat(assertions.query(
                    """
                    SELECT count_values FROM
                           (SELECT ARRAY[BIGINT '1', BIGINT '2'] AS a FROM (VALUES BIGINT '0') l(k) RIGHT JOIN (VALUES BIGINT '0', BIGINT '1') r(k) ON l.k = r.k) t
                           CROSS JOIN LATERAL (
                           SELECT count(*) AS count_values
                           FROM UNNEST(a) u(v))
                    """))
                    .matches("VALUES BIGINT '2', BIGINT '2'");
        }
    }
}
