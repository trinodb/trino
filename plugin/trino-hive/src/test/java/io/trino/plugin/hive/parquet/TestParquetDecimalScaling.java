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
package io.trino.plugin.hive.parquet;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.parquet.write.TestingMapredParquetOutputFormat;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.schema.MessageType;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.transform;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.parquet.TestParquetDecimalScaling.ParquetDecimalInsert.maximumValue;
import static io.trino.plugin.hive.parquet.TestParquetDecimalScaling.ParquetDecimalInsert.minimumValue;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Arrays.stream;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion;
import static org.apache.parquet.hadoop.ParquetOutputFormat.COMPRESSION;
import static org.apache.parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetDecimalScaling
        extends AbstractTestQueryFramework
{
    private java.nio.file.Path basePath;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        basePath = getBasePath();

        return HiveQueryRunner.builder()
                // create nation so tpch schema got created
                .setInitialTables(ImmutableList.of(NATION))
                .setBaseDataDir(Optional.of(basePath))
                .build();
    }

    /**
     * Tests if Parquet decimal with given precision and scale can be read into Trino decimal with different precision and scale
     * if Parquet decimal value could be rescaled into Trino decimal without losing most and least significant digits.
     */
    @Test(dataProvider = "testReadingMatchingPrecisionDataProvider")
    public void testReadingMatchingPrecision(
            int precision,
            int scale,
            boolean forceFixedLengthArray,
            List<String> values,
            List<String> expected,
            WriterVersion writerVersion)
    {
        String tableName = generateTableName("matching_precision", precision, scale);

        createTable(tableName, precision, scale);

        writeParquetDecimalsRecord(
                getParquetWritePath(tableName),
                ImmutableList.of(new ParquetDecimalInsert("value", forceFixedLengthArray, precision, scale, values)),
                writerVersion);

        assertValues(tableName, scale, expected);

        dropTable(tableName);
    }

    @DataProvider
    public Object[][] testReadingMatchingPrecisionDataProvider()
    {
        return withWriterVersion(new Object[][] {
                {10, 2, false,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.01", "10.00", "1.23", maximumValue(10, 2), minimumValue(10, 2))},
                {10, 2, true,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.01", "10.00", "1.23", maximumValue(10, 2), minimumValue(10, 2))},
                {4, 2, false,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.01", "10.00", "1.23", maximumValue(4, 2), minimumValue(4, 2))},
                {4, 2, true,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.01", "10.00", "1.23", maximumValue(4, 2), minimumValue(4, 2))},
                {14, 2, false,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(14, 2), minimumValue(14, 2)),
                        ImmutableList.of("10.01", "10.00", "1.23", maximumValue(14, 2), minimumValue(14, 2))},
                {6, 3, false,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(6, 3), minimumValue(6, 3)),
                        ImmutableList.of("10.010", "10.000", "1.230", maximumValue(6, 3), minimumValue(6, 3))},
                {6, 3, true,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(6, 3), minimumValue(6, 3)),
                        ImmutableList.of("10.010", "10.000", "1.230", maximumValue(6, 3), minimumValue(6, 3))},
                {38, 4, false,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(38, 4), minimumValue(38, 4)),
                        ImmutableList.of("10.0100", "10.0000", "1.2300", maximumValue(38, 4), minimumValue(38, 4))}
        });
    }

    /**
     * Tests if Parquet decimal with given precision and scale can be read into Trino decimal with different precision and scale
     * if Parquet decimal value could be rescaled into Trino decimal without loosing most and least significant digits.
     */
    @Test(dataProvider = "testReadingRescaledDecimalsProvider")
    public void testReadingRescaledDecimals(
            int precision,
            int scale,
            boolean forceFixedLengthArray,
            int schemaPrecision,
            int schemaScale,
            List<String> values,
            List<String> expected,
            WriterVersion writerVersion)
    {
        String tableName = generateTableName("rescaled_decimals", precision, scale);

        createTable(tableName, schemaPrecision, schemaScale);

        writeParquetDecimalsRecord(
                getParquetWritePath(tableName),
                ImmutableList.of(new ParquetDecimalInsert("value", forceFixedLengthArray, precision, scale, values)),
                writerVersion);

        assertValues(tableName, schemaScale, expected);

        dropTable(tableName);
    }

    @DataProvider
    public Object[][] testReadingRescaledDecimalsProvider()
    {
        // parquetPrecision, parquetScale, useFixedLengthArray, schemaPrecision, schemaScale, writeValues, expectedValues
        return withWriterVersion(new Object[][] {
                {10, 2, false, 12, 4,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.0100", "10.0000", "1.2300", maximumValue(10, 2), minimumValue(10, 2))},
                {10, 2, true, 13, 5,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.01000", "10.0000", "1.23000", maximumValue(10, 2), minimumValue(10, 2))},
                {4, 2, false, 6, 4,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.0100", "10.0000", "1.2300", maximumValue(4, 2), minimumValue(4, 2))},
                {4, 2, false, 6, 2,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.01", "10.00", "1.23", maximumValue(4, 2), minimumValue(4, 2))},
                {10, 2, false, 11, 3,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.010", "10.000", "1.230", maximumValue(10, 2), minimumValue(10, 2))},
                {10, 2, true, 12, 4,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.0100", "10.0000", "1.2300", maximumValue(10, 2), minimumValue(10, 2))},
                {4, 2, false, 10, 5,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.01000", "10.00000", "1.23000", maximumValue(4, 2), minimumValue(4, 2))},
                {4, 2, true, 10, 5,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.01000", "10.00000", "1.23000", maximumValue(4, 2), minimumValue(4, 2))},
                {14, 2, false, 20, 3,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(14, 2), minimumValue(14, 2)),
                        ImmutableList.of("10.010", "10.000", "1.230", maximumValue(14, 2), minimumValue(14, 2))},
                {6, 3, false, 9, 6,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(6, 3), minimumValue(6, 3)),
                        ImmutableList.of("10.010000", "10.000000", "1.230000", maximumValue(6, 3), minimumValue(6, 3))},
                {6, 3, true, 9, 6,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(6, 3), minimumValue(6, 3)),
                        ImmutableList.of("10.010000", "10.000000", "1.230000", maximumValue(6, 3), minimumValue(6, 3))},
                {10, 2, false, 38, 4,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.0100", "10.0000", "1.2300", maximumValue(10, 2), minimumValue(10, 2))},
                {18, 4, false, 38, 14,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(18, 4), minimumValue(18, 4)),
                        ImmutableList.of("10.0100", "10.0000", "1.2300", maximumValue(18, 4), minimumValue(18, 4))},
        });
    }

    /**
     * Tests if Parquet decimal with given precision and scale can be read into Trino decimal with different precision and scale
     * if Parquet decimal value will be rounded to fit into Trino decimal.
     */
    @Test(dataProvider = "testReadingRoundedDecimalsProvider")
    public void testReadingRoundedDecimals(
            int precision,
            int scale,
            boolean forceFixedLengthArray,
            int schemaPrecision,
            int schemaScale,
            List<String> values,
            List<String> expected,
            WriterVersion writerVersion)
    {
        String tableName = generateTableName("rounded_decimals", precision, scale);

        createTable(tableName, schemaPrecision, schemaScale);

        writeParquetDecimalsRecord(
                getParquetWritePath(tableName),
                ImmutableList.of(new ParquetDecimalInsert("value", forceFixedLengthArray, precision, scale, values)),
                writerVersion);

        assertRoundedValues(tableName, schemaScale, expected);

        dropTable(tableName);
    }

    @DataProvider
    public Object[][] testReadingRoundedDecimalsProvider()
    {
        // parquetPrecision, parquetScale, useFixedLengthArray, schemaPrecision, schemaScale, writeValues, expectedValues
        return withWriterVersion(new Object[][] {
                {10, 2, false, 12, 1,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2))},
                {9, 2, true, 12, 1,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(9, 2), minimumValue(9, 2)),
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(9, 2), minimumValue(9, 2))},
                {4, 2, false, 7, 1,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2)),
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(4, 2), minimumValue(4, 2))},
                {10, 2, false, 12, 1,
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2)),
                        ImmutableList.of("10.01", "10", "1.23", maximumValue(10, 2), minimumValue(10, 2))},
        });
    }

    /**
     * Tests if Parquet decimal with given precision and scale cannot be read into Presto decimal with different precision and scale
     * because when rescaling decimal we would loose most significant digits.
     */
    @Test(dataProvider = "testReadingNonRescalableDecimalsProvider")
    public void testReadingNonRescalableDecimals(
            int precision,
            int scale,
            boolean forceFixedLengthArray,
            int schemaPrecision,
            int schemaScale,
            List<String> values,
            WriterVersion writerVersion)
    {
        String tableName = generateTableName("non_rescalable", precision, scale);

        createTable(tableName, schemaPrecision, schemaScale);

        writeParquetDecimalsRecord(
                getParquetWritePath(tableName),
                ImmutableList.of(new ParquetDecimalInsert("value", forceFixedLengthArray, precision, scale, values)),
                writerVersion);

        @Language("SQL") String query = format("SELECT * FROM tpch.%s", tableName);
        @Language("RegExp") String expectedMessage = format("Cannot cast DECIMAL\\(%d, %d\\) '.*' to DECIMAL\\(%d, %d\\)", precision, scale, schemaPrecision, schemaScale);

        assertQueryFails(optimizedParquetReaderEnabled(false), query, expectedMessage);
        assertQueryFails(optimizedParquetReaderEnabled(true), query, expectedMessage);

        dropTable(tableName);
    }

    @DataProvider
    public Object[][] testReadingNonRescalableDecimalsProvider()
    {
        // parquetPrecision, parquetScale, useFixedLengthArray, schemaPrecision, schemaScale, writeValues
        return withWriterVersion(new Object[][] {
                {4, 2, false, 4, 3, ImmutableList.of("10.01")},
                {10, 2, false, 10, 3, ImmutableList.of("12345678.91")},
                {10, 2, false, 3, 2, ImmutableList.of("10.01")},
                {10, 2, true, 14, 7, ImmutableList.of("99999999.99")},
                {10, 2, false, 10, 4, ImmutableList.of("99999999.99")},
                {18, 8, false, 32, 23, ImmutableList.of("1234567890.12345678")},
                {20, 8, false, 32, 21, ImmutableList.of("123456789012.12345678")},
        });
    }

    @Test(dataProvider = "testParquetLongFixedLenByteArrayWithTrinoShortDecimalProvider")
    public void testParquetLongFixedLenByteArrayWithTrinoShortDecimal(
            int schemaPrecision,
            int schemaScale,
            int parquetPrecision,
            int parquetScale,
            String writeValue,
            WriterVersion writerVersion)
    {
        String tableName = generateTableName("rounded_decimals", parquetPrecision, parquetScale);
        createTable(tableName, schemaPrecision, schemaScale);

        int byteArrayLength = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[parquetPrecision - 1];
        MessageType schema = parseMessageType(format(
                "message hive_record { optional fixed_len_byte_array(%d) value (DECIMAL(%d, %d)); }",
                byteArrayLength,
                schemaPrecision,
                schemaScale));
        List<ObjectInspector> inspectors = ImmutableList.of(new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(parquetPrecision, parquetScale)));

        createParquetFile(
                getParquetWritePath(tableName),
                getStandardStructObjectInspector(ImmutableList.of("value"), inspectors),
                new Iterator[] {ImmutableList.of(HiveDecimal.create(writeValue)).stream().iterator()},
                schema,
                Collections.singletonList("hive_record"),
                writerVersion);

        if (overflows(new BigDecimal(writeValue).unscaledValue(), schemaPrecision)) {
            @Language("SQL") String query = format("SELECT * FROM tpch.%s", tableName);
            @Language("RegExp") String expectedMessage = format(
                    "Could not read unscaled value %s into a short decimal from column .*",
                    new BigDecimal(writeValue).unscaledValue());

            assertQueryFails(optimizedParquetReaderEnabled(false), query, expectedMessage);
            assertQueryFails(optimizedParquetReaderEnabled(true), query, expectedMessage);
        }
        else {
            assertValues(tableName, schemaScale, ImmutableList.of(writeValue));
        }

        dropTable(tableName);
    }

    @DataProvider
    public Object[][] testParquetLongFixedLenByteArrayWithTrinoShortDecimalProvider()
    {
        // schemaPrecision, schemaScale, parquetPrecision, parquetScale, writeValue
        return withWriterVersion(new Object[][] {
                {5, 2, 19, 2, "-5"},
                {5, 2, 20, 2, "999.99"},
                {7, 2, 24, 2, "-99999.99"},
                {10, 2, 26, 2, "99999999.99"},
                {14, 4, 30, 4, "99999999.99"},
                {18, 8, 32, 8, "1234567890.12345678"},
                {18, 8, 32, 8, "123456789012.12345678"},
                {18, 8, 38, 8, "4989875563210.12345678"},
        });
    }

    protected void createTable(String tableName, int precision, int scale)
    {
        assertUpdate(format("CREATE TABLE tpch.%s (value decimal(%d, %d)) WITH (format = 'PARQUET')", tableName, precision, scale));
    }

    protected void dropTable(String tableName)
    {
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    private void assertValues(String tableName, int scale, List<String> expected)
    {
        assertValues(optimizedParquetReaderEnabled(false), tableName, scale, expected);
        assertValues(optimizedParquetReaderEnabled(true), tableName, scale, expected);
    }

    private void assertValues(Session session, String tableName, int scale, List<String> expected)
    {
        MaterializedResult materializedRows = computeActual(session, format("SELECT value FROM tpch.%s", tableName));

        List<BigDecimal> actualValues = materializedRows.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .map(BigDecimal.class::cast)
                .collect(toImmutableList());

        BigDecimal[] expectedValues = expected.stream()
                .map(value -> new BigDecimal(value).setScale(scale, UNNECESSARY))
                .toArray(BigDecimal[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expectedValues);
    }

    private void assertRoundedValues(String tableName, int scale, List<String> expected)
    {
        assertRoundedValues(optimizedParquetReaderEnabled(false), tableName, scale, expected);
        assertRoundedValues(optimizedParquetReaderEnabled(true), tableName, scale, expected);
    }

    private void assertRoundedValues(Session session, String tableName, int scale, List<String> expected)
    {
        MaterializedResult materializedRows = computeActual(session, format("SELECT value FROM tpch.%s", tableName));

        List<BigDecimal> actualValues = materializedRows.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .map(BigDecimal.class::cast)
                .collect(toImmutableList());

        BigDecimal[] expectedValues = expected.stream()
                .map(value -> new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP))
                .toArray(BigDecimal[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expectedValues);
    }

    private static java.nio.file.Path getBasePath()
    {
        try {
            return Files.createTempDirectory("parquet");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getParquetWritePath(String tableName)
    {
        return new Path(basePath.toString(), format("hive_data/tpch/%s/%s", tableName, UUID.randomUUID()));
    }

    private static void createParquetFile(
            Path path,
            StandardStructObjectInspector inspector,
            Iterator<?>[] iterators,
            MessageType parquetSchema,
            List<String> columnNames,
            WriterVersion writerVersion)
    {
        Properties tableProperties = createTableProperties(columnNames, Collections.singletonList(inspector));

        JobConf jobConf = new JobConf(newEmptyConfiguration());
        jobConf.setEnum(COMPRESSION, UNCOMPRESSED);
        jobConf.setBoolean(ENABLE_DICTIONARY, false);
        jobConf.setEnum(WRITER_VERSION, writerVersion);

        try {
            FileSinkOperator.RecordWriter recordWriter = new TestingMapredParquetOutputFormat(Optional.of(parquetSchema), true, DateTimeZone.getDefault())
                    .getHiveRecordWriter(
                            jobConf,
                            path,
                            Text.class,
                            false,
                            tableProperties,
                            () -> {});

            Object row = inspector.create();
            List<StructField> fields = ImmutableList.copyOf(inspector.getAllStructFieldRefs());

            while (stream(iterators).allMatch(Iterator::hasNext)) {
                for (int i = 0; i < fields.size(); i++) {
                    Object value = iterators[i].next();
                    inspector.setStructFieldData(row, fields.get(i), value);
                }

                ParquetHiveSerDe serde = new ParquetHiveSerDe();
                serde.initialize(jobConf, tableProperties, null);
                Writable record = serde.serialize(row, inspector);
                recordWriter.write(record);
            }

            recordWriter.close(false);
        }
        catch (IOException | SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeParquetDecimalsRecord(Path output, List<ParquetDecimalInsert> inserts, WriterVersion writerVersion)
    {
        List<String> fields = inserts.stream().map(ParquetDecimalInsert::schemaFieldDeclaration).collect(toImmutableList());
        MessageType schema = parseMessageType(format("message hive_record { %s; }", Joiner.on("; ").join(fields)));
        List<ObjectInspector> inspectors = inserts.stream().map(ParquetDecimalInsert::getParquetObjectInspector).collect(toImmutableList());
        List<String> columnNames = inserts.stream().map(ParquetDecimalInsert::getColumnName).collect(toImmutableList());
        Iterator<?>[] values = inserts.stream().map(ParquetDecimalInsert::getValues).map(Iterable::iterator).toArray(Iterator[]::new);

        createParquetFile(
                output,
                getStandardStructObjectInspector(columnNames, inspectors),
                values,
                schema,
                Collections.singletonList("hive_record"),
                writerVersion);
    }

    private static Properties createTableProperties(List<String> columnNames, List<ObjectInspector> objectInspectors)
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", Joiner.on(',').join(columnNames));
        tableProperties.setProperty("columns.types", Joiner.on(',').join(transform(objectInspectors, ObjectInspector::getTypeName)));
        return tableProperties;
    }

    private static String generateTableName(String testCase, int precision, int scale)
    {
        return format("%s_%d_%d_%d", testCase, precision, scale, ThreadLocalRandom.current().nextInt(1, MAX_VALUE));
    }

    private static Object[][] withWriterVersion(Object[][] args)
    {
        Object[][] versions = Stream.of(WriterVersion.values())
                .collect(toDataProvider());
        return cartesianProduct(args, versions);
    }

    private Session optimizedParquetReaderEnabled(boolean enabled)
    {
        Session session = getSession();
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_optimized_reader_enabled", Boolean.toString(enabled))
                .build();
    }

    protected static class ParquetDecimalInsert
    {
        private final String columnName;
        private final boolean forceFixedLengthArray;
        private final int precision;
        private final int scale;
        private final List<String> values;

        public ParquetDecimalInsert(String columnName, boolean forceFixedLengthArray, int precision, int scale, List<String> values)
        {
            this.columnName = columnName;
            this.forceFixedLengthArray = forceFixedLengthArray;
            this.precision = precision;
            this.scale = scale;
            this.values = values;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public String parquetStorage()
        {
            if (!forceFixedLengthArray && precision > 0 && precision < 10) {
                return "INT32";
            }

            if (!forceFixedLengthArray && precision >= 10 && precision < 18) {
                return "INT64";
            }

            if (precision > 38 || precision < 0) {
                throw new IllegalArgumentException("Scale cannot be greater than 38 or less than 0");
            }

            return format("fixed_len_byte_array(%d)", ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[precision - 1]);
        }

        public ObjectInspector getParquetObjectInspector()
        {
            if (!forceFixedLengthArray && precision > 0 && precision < 10) {
                return javaIntObjectInspector;
            }

            if (!forceFixedLengthArray && precision >= 10 && precision < 18) {
                return javaLongObjectInspector;
            }

            if (precision > 38 || precision < 0) {
                throw new IllegalArgumentException("Scale cannot be greater than 38 or less than 0");
            }

            return new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(precision, scale));
        }

        public String schemaFieldDeclaration()
        {
            return format("optional %s %s (DECIMAL(%d, %d))", parquetStorage(), columnName, precision, scale);
        }

        public static String maximumValue(int precision, int scale)
        {
            return format("%s.%s", "9".repeat(precision - scale), "9".repeat(scale));
        }

        public static String minimumValue(int precision, int scale)
        {
            return "-" + maximumValue(precision, scale);
        }

        public Iterable<?> getValues()
        {
            ImmutableList<String> inserts = ImmutableList.copyOf(values);

            return inserts.stream().map(this::convertValue).collect(toImmutableList());
        }

        private Object convertValue(String value)
        {
            BigDecimal bigValue = new BigDecimal(value).setScale(scale, UNNECESSARY);

            if (!forceFixedLengthArray && precision > 0 && precision < 10) {
                return bigValue.unscaledValue().intValue();
            }

            if (!forceFixedLengthArray && precision >= 10 && precision < 18) {
                return bigValue.unscaledValue().longValue();
            }

            if (precision > 38 || precision < 0) {
                throw new IllegalArgumentException("Scale could not be greater than 38 or less than 0");
            }

            return HiveDecimal.create(bigValue);
        }
    }
}
