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
package io.prestosql.plugin.iceberg.fileformat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.assertions.Assert;

import java.util.List;

import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergOrcIntegrationSmoke
        extends AbstractTestIntegrationSmokeTest
{
    public static final List<MaterializedRow> ORDERS_COLUMNS = ImmutableList.<MaterializedRow>builder()
            .add(new MaterializedRow(DEFAULT_PRECISION, "orderkey", "bigint", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "custkey", "bigint", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "orderstatus", "varchar", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "totalprice", "double", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "orderdate", "date", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "orderpriority", "varchar", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "clerk", "varchar", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "shippriority", "integer", "", ""))
            .add(new MaterializedRow(DEFAULT_PRECISION, "comment", "varchar", "", ""))
            .build();
    public static final String SHOW_CREATE_TABLE_ORDERS_RESULT = "CREATE TABLE iceberg.tpch.orders (\n" +
            "   orderkey bigint,\n" +
            "   custkey bigint,\n" +
            "   orderstatus varchar,\n" +
            "   totalprice double,\n" +
            "   orderdate date,\n" +
            "   orderpriority varchar,\n" +
            "   clerk varchar,\n" +
            "   shippriority integer,\n" +
            "   comment varchar\n" +
            ")\n" +
            "WITH (\n" +
            "   format = 'ORC'\n" +
            ")";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of("iceberg.file-format", "orc"), true);
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .rows(ORDERS_COLUMNS)
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        Assert.assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(SHOW_CREATE_TABLE_ORDERS_RESULT);
    }
}
