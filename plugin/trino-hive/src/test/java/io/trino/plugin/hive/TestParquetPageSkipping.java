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
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
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

    @Test
    public void testTopNPrunesRowGroups()
    {
        String table = "test_topn_" + randomNameSuffix();
        Session writeSession = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_writer_row_group_max_row_count", "1000")
                .build();
        try {
            assertUpdate(
                    writeSession,
                    """
                    CREATE TABLE %s WITH (format = 'PARQUET', bucketed_by = ARRAY['bucket'], bucket_count = 1, sorted_by = ARRAY['key'])
                    AS SELECT CAST(x AS bigint) AS key, 0 AS bucket FROM UNNEST(sequence(1, 8000)) t(x)
                    """.formatted(table),
                    8000);
            assertQuery("SELECT count(DISTINCT \"$path\") FROM " + table, "VALUES 1");
            String sql = "SELECT key FROM " + table + " ORDER BY key LIMIT 3";
            Session disabled = Session.builder(getSession()).setSystemProperty("enable_dynamic_filtering", "false").build();
            assertQueryStats(
                    disabled,
                    sql,
                    stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(8000),
                    result -> assertThat(result.getOnlyColumn()).containsExactly(1L, 2L, 3L));
            assertQueryStats(getSession(),
                    sql,
                    stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(1000),
                    result -> assertThat(result.getOnlyColumn()).containsExactly(1L, 2L, 3L));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testTopNPrunesRowGroupsForScalarTypes()
    {
        String table = "test_topn_types_" + randomNameSuffix();
        Session writer = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_writer_row_group_max_row_count", "1000")
                .build();
        try {
            assertUpdate(
                    writer,
                    """
                    CREATE TABLE %s WITH (format = 'PARQUET', bucketed_by = ARRAY['bucket'], bucket_count = 1, sorted_by = ARRAY['id'])
                    AS SELECT x AS id, 0 AS bucket, x > 4000 AS flag, CAST(x / 100 AS tinyint) AS tiny,
                        CAST(x AS smallint) AS small, CAST(x AS real) AS r, CAST(x AS double) AS d,
                        CAST(x AS decimal(8,2)) AS short_decimal, CAST(x AS decimal(30,2)) AS long_decimal,
                        date_add('day', x, DATE '1960-01-01') AS dt,
                        lpad(CAST(x AS varchar), 5, '0') AS str,
                        IF(x <= 1000, to_big_endian_64(x)) AS bin
                    FROM UNNEST(sequence(1, 8000)) t(x)
                    """.formatted(table),
                    8000);
            Session disabled = Session.builder(getSession()).setSystemProperty("enable_dynamic_filtering", "false").build();
            for (String key : List.of("id", "flag", "tiny", "small", "r", "d", "short_decimal", "long_decimal", "dt", "str", "bin")) {
                String query = "SELECT " + key + " FROM " + table + " ORDER BY " + key + " LIMIT 3";
                MaterializedResult expected = computeActual(disabled, query);
                assertQueryStats(getSession(),
                        query,
                        stats -> assertThat(stats.getPhysicalInputPositions()).as(key).isBetween(3L, 7999L),
                        result -> assertThat(result.getMaterializedRows()).as(key).isEqualTo(expected.getMaterializedRows()));
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testTopNWithCoercedParquetColumns()
    {
        String source = "test_topn_source_" + randomNameSuffix();
        String table = "test_topn_coerced_" + randomNameSuffix();
        Session writer = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_writer_row_group_max_row_count", "1000")
                .build();
        try {
            assertUpdate(
                    writer,
                    """
                    CREATE TABLE %s WITH (format = 'PARQUET', bucketed_by = ARRAY['bucket'], bucket_count = 1, sorted_by = ARRAY['id'])
                    AS SELECT x AS id, CAST(x AS real) AS r, concat('a', CAST(x AS varchar)) AS s, 0 AS bucket
                    FROM UNNEST(sequence(1, 8000)) t(x)
                    """.formatted(source),
                    8000);
            String file = (String) computeScalar("SELECT DISTINCT \"$path\" FROM " + source);
            assertUpdate("CREATE TABLE %s (id varchar, r double, s varchar(1), bucket integer) WITH (format = 'PARQUET', external_location = '%s')"
                    .formatted(table, Location.of(file).parentDirectory()));

            Session disabled = Session.builder(getSession()).setSystemProperty("enable_dynamic_filtering", "false").build();
            for (String ordering : List.of("id", "r", "s, r DESC")) {
                String sql = "SELECT id FROM " + table + " ORDER BY " + ordering + " LIMIT 3";
                assertThat(computeActual(sql).getMaterializedRows())
                        .as(ordering)
                        .isEqualTo(computeActual(disabled, sql).getMaterializedRows());
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
            assertUpdate("DROP TABLE IF EXISTS " + source);
        }
    }

    @Test
    public void testSelectedPositionsPushdown()
    {
        String tableName = "test_selected_positions_pushdown_" + randomNameSuffix();
        try {
            assertUpdate(
                    """
                    CREATE TABLE %s (
                        filter_key bigint,
                        payload varchar,
                        bucket integer)
                    WITH (
                        format = 'PARQUET',
                        bucketed_by = ARRAY['bucket'],
                        bucket_count = 1,
                        sorted_by = ARRAY['filter_key'])
                    """.formatted(tableName));
            assertUpdate(
                    """
                    INSERT INTO %s
                    SELECT value, CAST(value AS varchar) || 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 0
                    FROM UNNEST(sequence(0, 8191)) AS t(value)
                    """.formatted(tableName),
                    8_192);

            MaterializedResult allRows = computeActual("SELECT filter_key, payload FROM " + tableName + " ORDER BY filter_key");
            List<MaterializedRow> expectedRows = allRows.getMaterializedRows().stream()
                    .filter(row -> ((long) row.getField(0) % 1_024) >= 400)
                    .filter(row -> ((long) row.getField(0) % 1_024) <= 407)
                    .toList();
            MaterializedResult selectedRows = computeActual(
                    "SELECT filter_key, payload FROM " + tableName + " WHERE filter_key % 1024 BETWEEN 400 AND 407 ORDER BY filter_key");

            assertThat(selectedRows.getMaterializedRows()).containsExactlyElementsOf(expectedRows);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
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
