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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.testng.Assert.assertEquals;

public class TestIcebergParquetConnectorTest
        extends BaseIcebergConnectorTest
{
    public TestIcebergParquetConnectorTest()
    {
        super(PARQUET);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return true;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary") ||
                typeName.equalsIgnoreCase("time(6)") ||
                typeName.equalsIgnoreCase("timestamp(6) with time zone"));
    }

    @Test
    public void testRowGroupResetDictionary()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_row_group_reset_dictionary",
                "(plain_col varchar, dict_col int)")) {
            String tableName = table.getName();
            String values = IntStream.range(0, 100)
                    .mapToObj(i -> "('ABCDEFGHIJ" + i + "' , " + (i < 20 ? "1" : "null") + ")")
                    .collect(Collectors.joining(", "));
            assertUpdate(withSmallRowGroups(getSession()), "INSERT INTO " + tableName + " VALUES " + values, 100);

            MaterializedResult result = getDistributedQueryRunner().execute(String.format("SELECT * FROM %s", tableName));
            assertEquals(result.getRowCount(), 100);
        }
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "parquet_writer_page_size", "100B")
                .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "100B")
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "10")
                .build();
    }
}
