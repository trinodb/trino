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
package io.trino.plugin.hive.functions.unload;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUnloadFunction
        extends AbstractTestQueryFramework
{
    private final Path tempDir = createTempDir();

    private static Path createTempDir()
    {
        try {
            return Files.createTempDirectory("trino-unload-test");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(ImmutableList.of(NATION))
                .addHiveProperty("hive.unload-enabled", "true")
                .setWorkerCount(0)
                .build();
    }

    // Supported formats for unload
    static Stream<String> supportedFormats()
    {
        return Stream.of("PARQUET", "ORC", "AVRO", "CSV", "JSON", "OPENX_JSON", "TEXTFILE");
    }

    // Unsupported formats for unload
    static Stream<String> unsupportedFormats()
    {
        return Stream.of("SEQUENCEFILE", "RCBINARY", "RCTEXT", "REGEX");
    }

    // Column type expressions and their SQL definitions for testing
    // CSV only supports VARCHAR columns, so non-VARCHAR types are excluded for CSV
    static Stream<Arguments> columnTypesAndFormats()
    {
        // Each argument: format, type label, column expression
        Stream<Arguments> args = Stream.empty();
        for (String format : new String[] {"PARQUET", "ORC", "AVRO", "JSON", "OPENX_JSON", "TEXTFILE"}) {
            args = Stream.concat(args, Stream.of(
                    Arguments.of(format, "boolean", "true"),
                    Arguments.of(format, "tinyint", "CAST(42 AS TINYINT)"),
                    Arguments.of(format, "smallint", "CAST(1234 AS SMALLINT)"),
                    Arguments.of(format, "integer", "12345"),
                    Arguments.of(format, "bigint", "CAST(1234567890 AS BIGINT)"),
                    Arguments.of(format, "real", "CAST(3.14 AS REAL)"),
                    Arguments.of(format, "double", "CAST(3.141592653589793 AS DOUBLE)"),
                    Arguments.of(format, "decimal_short", "CAST(3.14 AS DECIMAL(5,2))"),
                    Arguments.of(format, "decimal_long", "CAST('12345678901234567890.0123456789' AS DECIMAL(30,10))"),
                    Arguments.of(format, "varchar", "'hello world'"),
                    Arguments.of(format, "varchar_bounded", "CAST('bounded' AS VARCHAR(50))"),
                    Arguments.of(format, "char", "CAST('padded' AS CHAR(10))"),
                    Arguments.of(format, "varbinary", "CAST('binary' AS VARBINARY)"),
                    Arguments.of(format, "date", "DATE '2024-01-15'"),
                    Arguments.of(format, "timestamp", "TIMESTAMP '2024-01-15 12:30:45.123'")));
        }
        // CSV only supports unbounded VARCHAR columns
        args = Stream.concat(args, Stream.of(
                Arguments.of("CSV", "varchar", "CAST('hello world' AS VARCHAR)")));
        return args;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("supportedFormats")
    public void testUnloadWithFormat(String format)
    {
        String outputDir = tempDir.resolve("unload_format_" + format.toLowerCase(Locale.ROOT)).toUri().toString();
        // CSV only supports unbounded VARCHAR columns, so cast to VARCHAR for CSV
        String inputColumns = "CSV".equals(format) ? "CAST(name AS VARCHAR)" : "nationkey, name";
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT " + inputColumns + " FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => '" + format + "'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
        assertThat((long) result.getMaterializedRows().get(0).getField(2)).isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedFormats")
    public void testUnloadWithUnsupportedFormat(String format)
    {
        String outputDir = tempDir.resolve("unload_unsupported_" + format.toLowerCase(Locale.ROOT)).toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => '" + format + "'))",
                ".*Unsupported format for unload.*");
    }

    @ParameterizedTest(name = "{0} - {1}")
    @MethodSource("columnTypesAndFormats")
    public void testUnloadColumnType(String format, String typeLabel, String columnExpression)
    {
        String outputDir = tempDir.resolve("unload_type_" + format.toLowerCase(Locale.ROOT) + "_" + typeLabel).toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT " + columnExpression + " AS col), " +
                        "location => '" + outputDir + "', " +
                        "format => '" + format + "'))");
    }

    @Test
    public void testUnloadWithDefaultFormat()
    {
        String outputDir = tempDir.resolve("unload_default").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
        // Default format is PARQUET; verify file extension
        String path = (String) result.getMaterializedRows().get(0).getField(0);
        assertThat(path).endsWith(".parquet");
    }

    @Test
    public void testUnloadReturnsFileMetadata()
    {
        String outputDir = tempDir.resolve("unload_metadata").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
        assertThat((long) result.getMaterializedRows().get(0).getField(2)).isGreaterThan(0);
    }

    @Test
    public void testUnloadWithEmptyResult()
    {
        String outputDir = tempDir.resolve("unload_empty").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation WHERE nationkey < 0), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        // When no input data is provided, the result may have zero or one row
        if (result.getRowCount() > 0) {
            assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(0);
        }
    }

    @Test
    public void testUnloadWithInvalidFormat()
    {
        String outputDir = tempDir.resolve("unload_invalid").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'INVALID_FORMAT'))",
                ".*Unknown format.*");
    }

    @Test
    public void testUnloadWithEmptyLocation()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => ''))",
                ".*location must not be empty.*");
    }

    @Test
    public void testUnloadWithMultipleColumns()
    {
        String outputDir = tempDir.resolve("unload_multi_columns").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT" +
                        " nationkey," +
                        " name," +
                        " regionkey," +
                        " comment" +
                        " FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadWithMixedColumnTypes()
    {
        String outputDir = tempDir.resolve("unload_mixed_types").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT" +
                        " true AS bool_col," +
                        " CAST(1 AS TINYINT) AS tinyint_col," +
                        " CAST(2 AS SMALLINT) AS smallint_col," +
                        " 3 AS int_col," +
                        " CAST(4 AS BIGINT) AS bigint_col," +
                        " CAST(1.5 AS REAL) AS real_col," +
                        " CAST(2.5 AS DOUBLE) AS double_col," +
                        " CAST(3.14 AS DECIMAL(5,2)) AS decimal_col," +
                        " 'hello' AS varchar_col," +
                        " DATE '2024-01-01' AS date_col," +
                        " TIMESTAMP '2024-01-01 12:00:00' AS timestamp_col" +
                        "), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(1);
    }

    @Test
    public void testUnloadColumnNameSensitivity()
    {
        String outputDir = tempDir.resolve("unload_column_names").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT" +
                        " 1 AS \"MixedCase\"," +
                        " 2 AS \"ALLCAPS\"," +
                        " 3 AS \"lower\"," +
                        " 4 AS \"with_underscore\"," +
                        " 5 AS \"with123numbers\"" +
                        "), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(1);
    }

    @Test
    public void testUnloadLargeDataset()
    {
        String outputDir = tempDir.resolve("unload_large").toUri().toString();
        // Cross join nation (25 rows) with itself to get 625 rows
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT a.nationkey, a.name FROM " + HIVE_CATALOG + ".tpch.nation a CROSS JOIN " + HIVE_CATALOG + ".tpch.nation b), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(625);
        assertThat((long) result.getMaterializedRows().get(0).getField(2)).isGreaterThan(0);
    }

    @Test
    public void testUnloadToNonExistingDirectory()
    {
        // Nested non-existing directories
        String outputDir = tempDir.resolve("non_existing/deep/nested/dir").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
    }

    @Test
    public void testUnloadToNonEmptyDirectory()
    {
        // Write twice to the same directory
        String outputDir = tempDir.resolve("unload_non_empty").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        // Second write to the same directory should also succeed (distinct UUID filenames)
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadWithCompressionNone()
    {
        String outputDir = tempDir.resolve("unload_compress_none").toUri().toString();
        assertQuerySucceeds(
                "SET SESSION " + HIVE_CATALOG + ".compression_codec = 'NONE'");
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadWithCompressionSnappy()
    {
        String outputDir = tempDir.resolve("unload_compress_snappy").toUri().toString();
        assertQuerySucceeds(
                "SET SESSION " + HIVE_CATALOG + ".compression_codec = 'SNAPPY'");
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadWithCompressionGzip()
    {
        String outputDir = tempDir.resolve("unload_compress_gzip").toUri().toString();
        assertQuerySucceeds(
                "SET SESSION " + HIVE_CATALOG + ".compression_codec = 'GZIP'");
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadWithCompressionZstd()
    {
        String outputDir = tempDir.resolve("unload_compress_zstd").toUri().toString();
        assertQuerySucceeds(
                "SET SESSION " + HIVE_CATALOG + ".compression_codec = 'ZSTD'");
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadWithCompressionLz4()
    {
        String outputDir = tempDir.resolve("unload_compress_lz4").toUri().toString();
        assertQuerySucceeds(
                "SET SESSION " + HIVE_CATALOG + ".compression_codec = 'LZ4'");
        // LZ4 is supported for ORC
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'ORC'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
    }

    @Test
    public void testUnloadOrcFileExtension()
    {
        String outputDir = tempDir.resolve("unload_ext_orc").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'ORC'))");
        String path = (String) result.getMaterializedRows().get(0).getField(0);
        assertThat(path).endsWith(".orc");
    }

    @Test
    public void testUnloadAvroFileExtension()
    {
        String outputDir = tempDir.resolve("unload_ext_avro").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'AVRO'))");
        String path = (String) result.getMaterializedRows().get(0).getField(0);
        assertThat(path).endsWith(".avro");
    }

    @Test
    public void testUnloadCsvFileExtension()
    {
        String outputDir = tempDir.resolve("unload_ext_csv").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT CAST(name AS VARCHAR) AS name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'CSV'))");
        String path = (String) result.getMaterializedRows().get(0).getField(0);
        assertThat(path).endsWith(".csv");
    }

    @Test
    public void testUnloadJsonFileExtension()
    {
        String outputDir = tempDir.resolve("unload_ext_json").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'JSON'))");
        String path = (String) result.getMaterializedRows().get(0).getField(0);
        assertThat(path).endsWith(".json");
    }

    @Test
    public void testUnloadTextFileExtension()
    {
        String outputDir = tempDir.resolve("unload_ext_txt").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'TEXTFILE'))");
        String path = (String) result.getMaterializedRows().get(0).getField(0);
        assertThat(path).endsWith(".txt");
    }

    @Test
    public void testUnloadFormatCaseInsensitive()
    {
        String outputDir = tempDir.resolve("unload_case_insensitive").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'parquet'))");
    }

    @Test
    public void testUnloadCsvWithNonVarcharColumn()
    {
        // CSV only supports unbounded VARCHAR columns
        String outputDir = tempDir.resolve("unload_csv_nonvarchar").toUri().toString();
        assertThatThrownBy(() -> computeActual(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'CSV'))"))
                .hasMessageContaining("CSV only supports VARCHAR columns");
    }

    @Test
    public void testUnloadWithArrayType()
    {
        String outputDir = tempDir.resolve("unload_array_type").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT ARRAY[1, 2, 3] AS arr_col), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
    }

    @Test
    public void testUnloadWithMapType()
    {
        String outputDir = tempDir.resolve("unload_map_type").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]) AS map_col), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
    }

    @Test
    public void testUnloadWithRowType()
    {
        String outputDir = tempDir.resolve("unload_row_type").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT CAST(ROW(1, 'hello') AS ROW(x INTEGER, y VARCHAR)) AS row_col), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
    }

    @Test
    public void testUnloadWithSingleRow()
    {
        String outputDir = tempDir.resolve("unload_single_row").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT rows_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT 42 AS value), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }
}
