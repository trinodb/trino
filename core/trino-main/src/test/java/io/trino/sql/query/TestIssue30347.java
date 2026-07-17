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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIssue30347
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            Session session = assertions.sessionBuilder()
                    .setSystemProperty("iterative_optimizer_timeout", "1s")
                    .build();

            assertThat(assertions.query(
                    session,
                    """
                    WITH test_data(test_date, test_timestamp) AS (VALUES (DATE '2026-07-15', TIMESTAMP '2026-07-15'))
                    SELECT test_date = date_trunc('day', test_timestamp) FROM test_data
                    """))
                    .matches("VALUES true");

            assertThat(assertions.query(
                    session,
                    """
                    WITH test_data(test_year, test_timestamp) AS (VALUES (SMALLINT '2026', TIMESTAMP '2026-07-15'))
                    SELECT test_year = year(test_timestamp) FROM test_data
                    """))
                    .matches("VALUES true");
        }
    }
}
