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
package io.trino.tests;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.tpch.TpchTable.ORDERS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNumberAggregations
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(List.of(ORDERS))
                .build();
    }

    @Test
    public void testSum()
    {
        // simple cases
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES 1, 2, 3) t(i)"))
                .matches("VALUES NUMBER '6'");

        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES 1, -2, null) t(i)"))
                .matches("VALUES NUMBER '-1'");

        assertThat(query("SELECT g, sum(CAST(i as number)) FROM (VALUES ('a', 1), ('a', 41), ('b', NULL)) t(g, i) GROUP BY g"))
                .matches("VALUES ('a', NUMBER '42'), ('b', NULL)");

        // global aggregation
        assertThat(query("SELECT sum(CAST(custkey as number)) FROM orders"))
                .matches("SELECT CAST(sum(custkey) AS number) FROM orders");

        // grouped aggregation
        assertThat(query("SELECT sum(CAST(custkey as number)) FROM orders GROUP BY orderstatus"))
                .matches("SELECT CAST(sum(custkey) AS number) FROM orders GROUP BY orderstatus");

        // global with empty input
        assertThat(query("SELECT sum(CAST(custkey as number)) FROM orders WHERE false"))
                .matches("VALUES CAST(NULL AS number)");

        // Infinity
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES '1', '2', '3', '+Infinity') t(i)"))
                .matches("VALUES NUMBER '+Infinity'");
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES '1', '2', '3', '-Infinity') t(i)"))
                .matches("VALUES NUMBER '-Infinity'");
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES '1', '2', '+Infinity', '-Infinity') t(i)"))
                .matches("VALUES NUMBER 'NaN'");

        // NaN
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES '1', '2', '3', 'NaN') t(i)"))
                .matches("VALUES NUMBER 'NaN'");
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES '1', '+Infinity', '3', 'NaN') t(i)"))
                .matches("VALUES NUMBER 'NaN'");
        assertThat(query("SELECT sum(CAST(i as number)) FROM (VALUES '1', '-Infinity', '3', 'NaN') t(i)"))
                .matches("VALUES NUMBER 'NaN'");
    }

    @Test
    public void testSumWindow()
    {
        assertThat(query("SELECT sum(CAST(custkey AS number)) OVER () FROM orders"))
                .matches("SELECT CAST(sum(custkey) OVER () AS number) FROM orders");

        assertThat(query("SELECT sum(CAST(custkey AS number)) OVER (ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders"))
                .matches("SELECT CAST(sum(custkey) OVER (ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS number) FROM orders");

        assertThat(query("SELECT sum(CAST(custkey AS number)) OVER (ORDER BY orderkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM orders"))
                .matches("SELECT CAST(sum(custkey) OVER (ORDER BY orderkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS number) FROM orders");
    }

    @Test
    public void testMin()
    {
        assertThat(query("SELECT min(CAST(custkey as number)) FROM orders"))
                .matches("SELECT CAST(min(custkey) AS number) FROM orders");
    }

    @Test
    public void testMax()
    {
        assertThat(query("SELECT max(CAST(custkey as number)) FROM orders"))
                .matches("SELECT CAST(max(custkey) AS number) FROM orders");
    }

    @Test
    public void testAvg()
    {
        // global aggregation
        assertThat(query("SELECT avg(CAST(custkey as number)) FROM orders"))
                .matches("SELECT CAST(round(avg(custkey), 6) AS number) FROM orders");

        // grouped aggregation
        assertThat(query("SELECT avg(CAST(custkey as number)) FROM orders GROUP BY orderstatus"))
                .matches("SELECT CAST(round(avg(custkey), 6) AS number) FROM orders GROUP BY orderstatus");

        // global with empty input
        assertThat(query("SELECT avg(CAST(custkey as number)) FROM orders WHERE false"))
                .matches("VALUES CAST(NULL AS number)");
    }

    @Test
    public void testAvgWindow()
    {
        assertThat(query("SELECT avg(CAST(custkey AS number)) OVER () FROM orders"))
                .matches("SELECT CAST(round(avg(custkey) OVER (), 6) AS number) FROM orders");

        assertThat(query("SELECT avg(CAST(custkey AS number)) OVER (ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders"))
                .matches("SELECT CAST(round(avg(custkey) OVER (ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 6) AS number) FROM orders");

        assertThat(query("SELECT avg(CAST(custkey AS number)) OVER (ORDER BY orderkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM orders"))
                .matches("SELECT CAST(round(avg(custkey) OVER (ORDER BY orderkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 6) AS number) FROM orders");
    }
}
