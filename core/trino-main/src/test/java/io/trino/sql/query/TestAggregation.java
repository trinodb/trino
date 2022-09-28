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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestAggregation
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertThatThrownBy(() -> assertions.query("SELECT v > ALL (VALUES 1) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");

        assertThatThrownBy(() -> assertions.query("SELECT v > ANY (VALUES 1) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");

        assertThatThrownBy(() -> assertions.query("SELECT v > SOME (VALUES 1) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .hasMessageContaining("must be an aggregate expression or appear in GROUP BY clause");

        assertThat(assertions.query("SELECT count_if(v > ALL (VALUES 0, 1)) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query("SELECT count_if(v > ANY (VALUES 0, 1)) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .matches("VALUES BIGINT '2'");

        assertThatThrownBy(() -> assertions.query("SELECT 1 > ALL (VALUES k) FROM (VALUES (1, 1), (1, 2)) t(k, v) GROUP BY k"))
                .hasMessageContaining("line 1:17: Given correlated subquery is not supported");
    }
}
