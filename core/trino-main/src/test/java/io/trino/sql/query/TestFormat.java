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
public class TestFormat
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
    public void testAggregationInFormat()
    {
        assertThat(assertions.query("SELECT format('%.6f', sum(1000000 / 1e6))")).matches("SELECT cast(1.000000 as varchar)");
        assertThat(assertions.query("SELECT format('%.6f', avg(1))")).matches("SELECT cast(1.000000 as varchar)");
        assertThat(assertions.query("SELECT format('%d', count(1))")).matches("SELECT cast(1 as varchar)");
        assertThat(assertions.query("SELECT format('%d', arbitrary(1))")).matches("SELECT cast(1 as varchar)");
        assertThat(assertions.query("SELECT format('%s %s %s %s %s', sum(1), avg(1), count(1), max(1), min(1))")).matches("SELECT VARCHAR '1 1.0 1 1 1'");
        assertThat(assertions.query("SELECT format('%s', approx_distinct(1.0))")).matches("SELECT cast(1 as varchar)");

        assertThat(assertions.query("SELECT format('%d', cast(sum(totalprice) as bigint)) FROM (VALUES 20,99,15) t(totalprice)")).matches("SELECT CAST(sum(totalprice) as VARCHAR) FROM (VALUES 20,99,15) t(totalprice)");
        assertThat(assertions.query("SELECT format('%s', sum(k)) FROM (VALUES 1, 2, 3) t(k)")).matches("VALUES VARCHAR '6'");
        assertThat(assertions.query("SELECT format(arbitrary(s), sum(k)) FROM (VALUES ('%s', 1), ('%s', 2), ('%s', 3)) t(s, k)")).matches("VALUES VARCHAR '6'");

        assertThatThrownBy(() -> assertions.query("SELECT format(s, 1) FROM (VALUES ('%s', 1)) t(s, k) GROUP BY k"))
                .hasMessageMatching("\\Qline 1:8: 'format(s, 1)' must be an aggregate expression or appear in GROUP BY clause\\E");
    }
}
