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
package io.trino;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHiddenColumns
{
    private LocalQueryRunner runner;
    private QueryAssertions assertions;

    @BeforeAll
    public void setUp()
    {
        runner = LocalQueryRunner.create(TEST_SESSION);
        runner.createCatalog(TEST_SESSION.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void destroy()
    {
        if (runner != null) {
            runner.close();
            runner = null;
            assertions = null;
        }
    }

    @Test
    public void testDescribeTable()
    {
        assertThat(assertions.query("DESCRIBE region"))
                .matches(resultBuilder(TEST_SESSION, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("regionkey", "bigint", "", "")
                        .row("name", "varchar(25)", "", "")
                        .row("comment", "varchar(152)", "", "")
                        .build());
    }

    @Test
    public void testSimpleSelect()
    {
        assertThat(assertions.query("SELECT * FROM region")).matches("SELECT regionkey, name, comment FROM region");
        assertThat(assertions.query("SELECT *, row_number FROM region")).matches("SELECT regionkey, name, comment, row_number FROM region");
        assertThat(assertions.query("SELECT row_number, * FROM region")).matches("SELECT row_number, regionkey, name, comment FROM region");
        assertThat(assertions.query("SELECT *, row_number, * FROM region")).matches("SELECT regionkey, name, comment, row_number, regionkey, name, comment FROM region");
        assertThat(assertions.query("SELECT row_number, x.row_number FROM region x")).matches("SELECT row_number, row_number FROM region");
    }

    @Test
    public void testAliasedTableColumns()
    {
        // https://github.com/prestodb/presto/issues/11385
        // TPCH tables have a hidden "row_number" column, which triggers this bug.
        assertThat(assertions.query("SELECT * FROM orders AS t (a, b, c, d, e, f, g, h, i)"))
                .matches("SELECT * FROM orders");
    }
}
