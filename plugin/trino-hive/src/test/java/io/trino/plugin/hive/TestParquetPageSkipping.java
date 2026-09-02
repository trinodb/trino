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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.UUID;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetPageSkipping
        extends BaseTestParquetPageSkipping
{
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(
                        ImmutableMap.of(
                                "parquet.use-column-index", "true",
                                "parquet.max-buffer-size", "1MB"))
                .build();

        fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        return queryRunner;
    }

    @Override
    protected String createTableWithDataFile(String tableNamePrefix, String columnsDefinition, String resourceFileName)
            throws IOException
    {
        Location dataFile = copyInDataFile(resourceFileName);
        String tableName = tableName(tableNamePrefix);
        assertUpdate(format(
                "CREATE TABLE %s %s WITH (format = 'PARQUET', external_location = '%s')",
                tableName,
                columnsDefinition,
                dataFile.parentDirectory()));
        return tableName;
    }

    @Override
    protected String timestampMillisType()
    {
        return "timestamp(3)";
    }

    @Test
    public void testPageSkipping()
    {
        testPageSkipping("orderkey", "bigint", new Object[][] {{2, 7520, 7523, 14950}});
        testPageSkipping("totalprice", "double", new Object[][] {{974.04, 131094.34, 131279.97, 406938.36}});
        testPageSkipping("totalprice", "real", new Object[][] {{974.04, 131094.34, 131279.97, 406938.36}});
        testPageSkipping("totalprice", "decimal(12,2)", new Object[][] {
                {974.04, 131094.34, 131279.97, 406938.36},
                {973, 131095, 131280, 406950},
                {974.04123, 131094.34123, 131279.97012, 406938.36555},
        });
        testPageSkipping("totalprice", "decimal(12,0)", new Object[][] {
                {973, 131095, 131280, 406950},
        });
        testPageSkipping("totalprice", "decimal(35,2)", new Object[][] {
                {974.04, 131094.34, 131279.97, 406938.36},
                {973, 131095, 131280, 406950},
                {974.04123, 131094.34123, 131279.97012, 406938.36555},
        });
        testPageSkipping("orderdate", "date", new Object[][] {{"DATE '1992-01-05'", "DATE '1995-10-13'", "DATE '1995-10-13'", "DATE '1998-07-29'"}});
        testPageSkipping("orderdate", "timestamp", new Object[][] {{"TIMESTAMP '1992-01-05'", "TIMESTAMP '1995-10-13'", "TIMESTAMP '1995-10-14'", "TIMESTAMP '1998-07-29'"}});
        testPageSkipping("clerk", "varchar(15)", new Object[][] {{"'Clerk#000000006'", "'Clerk#000000508'", "'Clerk#000000513'", "'Clerk#000000996'"}});
        testPageSkipping("custkey", "integer", new Object[][] {{4, 634, 640, 1493}});
        testPageSkipping("custkey", "smallint", new Object[][] {{4, 634, 640, 1493}});
    }

    private void testPageSkipping(String sortByColumn, String sortByColumnType, Object[][] valuesArray)
    {
        String tableName = "test_page_skipping_" + randomNameSuffix();
        buildSortedTables(tableName, sortByColumn, sortByColumnType);
        for (Object[] values : valuesArray) {
            Object lowValue = values[0];
            Object middleLowValue = values[1];
            Object middleHighValue = values[2];
            Object highValue = values[3];
            assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s = %s", sortByColumn, tableName, sortByColumn, middleLowValue));
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s < %s", sortByColumn, tableName, sortByColumn, lowValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s > %s", sortByColumn, tableName, sortByColumn, highValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s BETWEEN %s AND %s", sortByColumn, tableName, sortByColumn, middleLowValue, middleHighValue))).isGreaterThan(0);
            // Tests synchronization of reading values across columns
            assertColumnIndexResults(format("SELECT * FROM %s WHERE %s = %s", tableName, sortByColumn, middleLowValue));
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s < %s", tableName, sortByColumn, lowValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s > %s", tableName, sortByColumn, highValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s BETWEEN %s AND %s", tableName, sortByColumn, middleLowValue, middleHighValue))).isGreaterThan(0);
            // Nested data
            assertColumnIndexResults(format("SELECT rvalues FROM %s WHERE %s IN (%s, %s, %s, %s)", tableName, sortByColumn, lowValue, middleLowValue, middleHighValue, highValue));
            // Without nested data
            assertColumnIndexResults(format("SELECT orderkey, orderdate FROM %s WHERE %s IN (%s, %s, %s, %s)", tableName, sortByColumn, lowValue, middleLowValue, middleHighValue, highValue));
        }
        assertUpdate("DROP TABLE " + tableName);
    }

    private void buildSortedTables(String tableName, String sortByColumnName, String sortByColumnType)
    {
        String createTableTemplate =
                "CREATE TABLE %s ( " +
                        "   orderkey bigint, " +
                        "   custkey bigint, " +
                        "   orderstatus varchar(1), " +
                        "   totalprice double, " +
                        "   orderdate date, " +
                        "   orderpriority varchar(15), " +
                        "   clerk varchar(15), " +
                        "   shippriority integer, " +
                        "   comment varchar(79), " +
                        "   rvalues double array " +
                        ") " +
                        "WITH ( " +
                        "   format = 'PARQUET', " +
                        "   bucketed_by = array['orderstatus'], " +
                        "   bucket_count = 1, " +
                        "   sorted_by = array['%s'] " +
                        ")";
        createTableTemplate = createTableTemplate.replaceFirst(sortByColumnName + "[ ]+([^,]*)", sortByColumnName + " " + sortByColumnType);

        assertUpdate(format(
                createTableTemplate,
                tableName,
                sortByColumnName));
        String catalog = getSession().getCatalog().orElseThrow();
        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "parquet_writer_page_size", "10000B")
                        .setCatalogSessionProperty(catalog, "parquet_writer_row_group_size", "2GB")
                        .build(),
                format("INSERT INTO %s SELECT *, ARRAY[rand(), rand(), rand()] FROM tpch.tiny.orders", tableName),
                15000);
    }

    private Location copyInDataFile(String resourceFileName)
            throws IOException
    {
        URL resourceLocation = Resources.getResource(resourceFileName);

        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("data.parquet");
        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }
        return dataFile;
    }
}
