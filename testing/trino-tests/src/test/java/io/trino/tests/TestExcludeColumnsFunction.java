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

import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExcludeColumnsFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }

    @Test
    public void testExcludeColumnsFunction()
    {
        assertThat(query("SELECT * FROM tpch.tiny.nation")).matches("SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation");

        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(comment)))
                """))
                .matches("SELECT nationkey, name, regionkey FROM tpch.tiny.nation");

        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(regionkey, nationkey)))
                """))
                .matches("SELECT name, comment FROM tpch.tiny.nation");
    }

    @Test
    public void testInvalidArgument()
    {
        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => CAST(null AS DESCRIPTOR)))
                """))
                .hasMessage("COLUMNS descriptor is null");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR()))
                """))
                .hasMessage("line 4:21: Invalid descriptor argument COLUMNS. Descriptors should be formatted as 'DESCRIPTOR(name [type], ...)'");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(foo, comment, bar)))
                """))
                .hasMessage("Excluded columns: [foo, bar] not present in the table");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(nationkey bigint, comment)))
                """))
                .hasMessage("COLUMNS descriptor contains types");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(nationkey, name, regionkey, comment)))
                """))
                .hasMessage("All columns are excluded");
    }

    @Test
    public void testColumnResolution()
    {
        // excluded column names are matched case-insensitive
        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, B, "c", "D", e),
                                    columns => DESCRIPTOR("A", "b", C, d)))
                """))
                .matches("SELECT 5");
    }

    @Test
    public void testReturnedColumnNames()
    {
        // the function preserves the incoming column names. (However, due to how the analyzer handles identifiers, these are not the canonical names according to the SQL identifier semantics.)
        assertThat(query("""
                SELECT a, b, c, d
                FROM TABLE(exclude_columns(
                                    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, B, "c", "D", e),
                                    columns => DESCRIPTOR(e)))
                """))
                .matches("SELECT 1, 2, 3, 4");
    }

    @Test
    public void testHiddenColumn()
    {
        assertThat(query("SELECT row_number FROM tpch.tiny.region")).matches("SELECT * FROM UNNEST(sequence(0, 4))");

        // the hidden column is not provided to the function
        assertThatThrownBy(() -> query("""
                SELECT row_number
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(comment)))
                """))
                .hasMessage("line 1:8: Column 'row_number' cannot be resolved");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(row_number)))
                """))
                .hasMessage("Excluded columns: [row_number] not present in the table");
    }

    @Test
    public void testAnonymousColumn()
    {
        // cannot exclude an unnamed columns. the unnamed columns are passed on unnamed.
        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(SELECT 1 a, 2, 3 c, 4),
                                    columns => DESCRIPTOR(a, c)))
                """))
                .matches("SELECT 2, 4");
    }

    @Test
    public void testDuplicateExcludedColumn()
    {
        // duplicates in excluded column names are allowed
        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(comment, name, comment)))
                """))
                .matches("SELECT nationkey, regionkey FROM tpch.tiny.nation");
    }

    @Test
    public void testDuplicateInputColumn()
    {
        // all input columns with given name are excluded
        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, b, c, a, b),
                                    columns => DESCRIPTOR(a, b)))
                """))
                .matches("SELECT 3");
    }

    @Test
    public void testFunctionResolution()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(system.builtin.exclude_columns(
                                    input => TABLE(tpch.tiny.nation),
                                    columns => DESCRIPTOR(comment)))
                """))
                .matches("""
                        SELECT *
                        FROM TABLE(exclude_columns(
                                            input => TABLE(tpch.tiny.nation),
                                            columns => DESCRIPTOR(comment)))
                        """);
    }

    @Test
    public void testBigInput()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(exclude_columns(
                                    input => TABLE(tpch.tiny.orders),
                                    columns => DESCRIPTOR(orderstatus, orderdate, orderpriority, clerk, shippriority, comment)))
                """))
                .matches("SELECT orderkey, custkey, totalprice FROM tpch.tiny.orders");
    }
}
