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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.metadata.InsertTableHandle;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.ColumnConstraint;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.EstimatedStatsAndCost;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.FormattedDomain;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.FormattedMarker;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.FormattedRange;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.IoPlan;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.IoPlan.TableColumnInfo;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import io.prestosql.tests.DistributedQueryRunner;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.prestosql.SystemSessionProperties.COLOCATED_JOIN;
import static io.prestosql.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static io.prestosql.SystemSessionProperties.DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION;
import static io.prestosql.SystemSessionProperties.GROUPED_EXECUTION;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.prestosql.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.prestosql.plugin.hive.HiveQueryRunner.createBucketedSession;
import static io.prestosql.plugin.hive.HiveQueryRunner.createQueryRunner;
import static io.prestosql.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.util.HiveUtil.columnExtraInfo;
import static io.prestosql.spi.predicate.Marker.Bound.EXACTLY;
import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final String catalog;
    private final Session bucketedSession;
    private final TypeTranslator typeTranslator;

    @SuppressWarnings("unused")
    public TestHiveIntegrationSmokeTest()
    {
        this(() -> createQueryRunner(ORDERS, CUSTOMER), createBucketedSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))), HIVE_CATALOG, new HiveTypeTranslator());
    }

    protected TestHiveIntegrationSmokeTest(QueryRunnerSupplier queryRunnerSupplier, Session bucketedSession, String catalog, TypeTranslator typeTranslator)
    {
        super(queryRunnerSupplier);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.bucketedSession = requireNonNull(bucketedSession, "bucketSession is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
    }

    @Test
    public void testSchemaOperations()
    {
        Session admin = Session.builder(getQueryRunner().getDefaultSession())
                .setIdentity(Identity.forUser("hive")
                        .withRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA new_schema");

        assertUpdate(admin, "CREATE TABLE new_schema.test (x bigint)");

        assertQueryFails(admin, "DROP SCHEMA new_schema", "Schema not empty: new_schema");

        assertUpdate(admin, "DROP TABLE new_schema.test");

        assertUpdate(admin, "DROP SCHEMA new_schema");
    }

    @Test
    public void testIOExplain()
    {
        // Test IO explain with small number of discrete components.
        computeActual("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['orderkey', 'processing']) AS SELECT custkey, orderkey, orderstatus = 'P' processing FROM orders WHERE orderkey < 3");

        EstimatedStatsAndCost estimate = new EstimatedStatsAndCost(2.0, 40.0, 40.0, 0.0, 0.0);
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_orders SELECT custkey, orderkey, processing FROM test_orders WHERE custkey <= 10");
        assertEquals(
                jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_orders"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT.getTypeSignature(),
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY)),
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "processing",
                                                        BOOLEAN.getTypeSignature(),
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("false"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("false"), EXACTLY)))))),
                                        estimate)),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_orders")),
                        estimate));

        assertUpdate("DROP TABLE test_orders");

        // Test IO explain with large number of discrete components where Domain::simpify comes into play.
        computeActual("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['orderkey']) AS SELECT custkey, orderkey FROM orders WHERE orderkey < 200");

        estimate = new EstimatedStatsAndCost(55.0, 990.0, 990.0, 0.0, 0.0);
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_orders SELECT custkey, orderkey + 10 FROM test_orders WHERE custkey <= 10");
        assertEquals(
                jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_orders"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT.getTypeSignature(),
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("199"), EXACTLY)))))),
                                        estimate)),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_orders")),
                        estimate));

        assertUpdate("DROP TABLE test_orders");
    }

    @Test
    public void testIoExplainWithPrimitiveTypes()
    {
        // Use LinkedHashMap to maintain insertion order for ease of locating
        // map entry if assertion in the loop below fails.
        Map<Object, TypeAndEstimate> data = new LinkedHashMap<>();
        data.put("foo", new TypeAndEstimate(createUnboundedVarcharType(), new EstimatedStatsAndCost(1.0, 16.0, 16.0, 0.0, 0.0)));
        data.put(Byte.toString((byte) (Byte.MAX_VALUE / 2)), new TypeAndEstimate(TINYINT, new EstimatedStatsAndCost(1.0, 10.0, 10.0, 0.0, 0.0)));
        data.put(Short.toString((short) (Short.MAX_VALUE / 2)), new TypeAndEstimate(SMALLINT, new EstimatedStatsAndCost(1.0, 11.0, 11.0, 0.0, 0.0)));
        data.put(Integer.toString(Integer.MAX_VALUE / 2), new TypeAndEstimate(IntegerType.INTEGER, new EstimatedStatsAndCost(1.0, 13.0, 13.0, 0.0, 0.0)));
        data.put(Long.toString(Long.MAX_VALUE / 2), new TypeAndEstimate(BIGINT, new EstimatedStatsAndCost(1.0, 17.0, 17.0, 0.0, 0.0)));
        data.put(Boolean.TRUE.toString(), new TypeAndEstimate(BOOLEAN, new EstimatedStatsAndCost(1.0, 10.0, 10.0, 0.0, 0.0)));
        data.put("bar", new TypeAndEstimate(createCharType(3), new EstimatedStatsAndCost(1.0, 16.0, 16.0, 0.0, 0.0)));
        data.put("1.2345678901234578E14", new TypeAndEstimate(DOUBLE, new EstimatedStatsAndCost(1.0, 17.0, 17.0, 0.0, 0.0)));
        data.put("123456789012345678901234.567", new TypeAndEstimate(createDecimalType(30, 3), new EstimatedStatsAndCost(1.0, 25.0, 25.0, 0.0, 0.0)));
        data.put("2019-01-01", new TypeAndEstimate(DateType.DATE, new EstimatedStatsAndCost(1.0, 13.0, 13.0, 0.0, 0.0)));
        data.put("2019-01-01 23:22:21.123", new TypeAndEstimate(TimestampType.TIMESTAMP, new EstimatedStatsAndCost(1.0, 17.0, 17.0, 0.0, 0.0)));
        int index = 0;
        for (Map.Entry<Object, TypeAndEstimate> entry : data.entrySet()) {
            index++;
            Type type = entry.getValue().type;
            EstimatedStatsAndCost estimate = entry.getValue().estimate;
            @Language("SQL") String query = format(
                    "CREATE TABLE test_types_table  WITH (partitioned_by = ARRAY['my_col']) AS " +
                            "SELECT 'foo' my_non_partition_col, CAST('%s' AS %s) my_col",
                    entry.getKey(),
                    type.getDisplayName());

            assertUpdate(query, 1);
            assertEquals(
                    jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM test_types_table").getOnlyColumnAsSet())),
                    new IoPlan(
                            ImmutableSet.of(new TableColumnInfo(
                                    new CatalogSchemaTableName(catalog, "tpch", "test_types_table"),
                                    ImmutableSet.of(
                                            new ColumnConstraint(
                                                    "my_col",
                                                    type.getTypeSignature(),
                                                    new FormattedDomain(
                                                            false,
                                                            ImmutableSet.of(
                                                                    new FormattedRange(
                                                                            new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY),
                                                                            new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY)))))),
                                    estimate)),
                            Optional.empty(),
                            estimate),
                    format("%d) Type %s ", index, type));

            assertUpdate("DROP TABLE test_types_table");
        }
    }

    @Test
    public void testReadNoColumns()
    {
        testWithAllStorageFormats(this::testReadNoColumns);
    }

    private void testReadNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        assertUpdate(session, format("CREATE TABLE test_read_no_columns WITH (format = '%s') AS SELECT 0 x", storageFormat), 1);
        assertQuery(session, "SELECT count(*) FROM test_read_no_columns", "SELECT 1");
        assertUpdate(session, "DROP TABLE test_read_no_columns");
    }

    @Test
    public void createTableWithEveryType()
    {
        @Language("SQL") String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 2 _integer" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('bar' AS CHAR(10)) _char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTable);
    }

    private void testCreatePartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ",  _varchar VARCHAR(65535)" +
                ", _char CHAR(10)" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _smallint SMALLINT" +
                ", _tinyint TINYINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _partition_string VARCHAR" +
                ", _partition_varchar VARCHAR(65535)" +
                ", _partition_char CHAR(10)" +
                ", _partition_tinyint TINYINT" +
                ", _partition_smallint SMALLINT" +
                ", _partition_integer INTEGER" +
                ", _partition_bigint BIGINT" +
                ", _partition_boolean BOOLEAN" +
                ", _partition_decimal_short DECIMAL(3,2)" +
                ", _partition_decimal_long DECIMAL(30,10)" +
                ", _partition_date DATE" +
                ", _partition_timestamp TIMESTAMP" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_string', '_partition_varchar', '_partition_char', '_partition_tinyint', '_partition_smallint', '_partition_integer', '_partition_bigint', '_partition_boolean', '_partition_decimal_short', '_partition_decimal_long', '_partition_date', '_partition_timestamp']" +
                ") ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of(
                "_partition_string",
                "_partition_varchar",
                "_partition_char",
                "_partition_tinyint",
                "_partition_smallint",
                "_partition_integer",
                "_partition_bigint",
                "_partition_boolean",
                "_partition_decimal_short",
                "_partition_decimal_long",
                "_partition_date",
                "_partition_timestamp");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
        }

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));
        assertColumnType(tableMetadata, "_partition_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_partition_varchar", createVarcharType(65535));

        MaterializedResult result = computeActual("SELECT * FROM test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", CAST(1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", 'foo' _partition_string" +
                ", 'bar' _partition_varchar" +
                ", CAST('boo' AS CHAR(10)) _partition_char" +
                ", CAST(1 AS TINYINT) _partition_tinyint" +
                ", CAST(1 AS SMALLINT) _partition_smallint" +
                ", 1 _partition_integer" +
                ", CAST (1 AS BIGINT) _partition_bigint" +
                ", true _partition_boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _partition_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _partition_decimal_long" +
                ", CAST('2017-05-01' AS DATE) _partition_date" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _partition_timestamp";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * FROM test_partitioned_table", select);
        assertQuery(session,
                "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _partition_string" +
                        " AND 'bar' = _partition_varchar" +
                        " AND CAST('boo' AS CHAR(10)) = _partition_char" +
                        " AND CAST(1 AS TINYINT) = _partition_tinyint" +
                        " AND CAST(1 AS SMALLINT) = _partition_smallint" +
                        " AND 1 = _partition_integer" +
                        " AND CAST(1 AS BIGINT) = _partition_bigint" +
                        " AND true = _partition_boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _partition_decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _partition_decimal_long" +
                        " AND CAST('2017-05-01' AS DATE) = _partition_date" +
                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _partition_timestamp",
                select);

        assertUpdate(session, "DROP TABLE test_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_partitioned_table"));
    }

    @Test
    public void createTableLike()
    {
        createTableLike("", false);
        createTableLike("EXCLUDING PROPERTIES", false);
        createTableLike("INCLUDING PROPERTIES", true);
    }

    private void createTableLike(String likeSuffix, boolean hasPartition)
    {
        // Create a non-partitioned table
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_table_original (" +
                "  tinyint_col tinyint " +
                ", smallint_col smallint" +
                ")";
        assertUpdate(createTable);

        // Verify the table is correctly created
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_table_original");
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);

        // Create a partitioned table
        @Language("SQL") String createPartitionedTable = "" +
                "CREATE TABLE test_partitioned_table_original (" +
                "  string_col VARCHAR" +
                ", decimal_long_col DECIMAL(30,10)" +
                ", partition_bigint BIGINT" +
                ", partition_decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY['partition_bigint', 'partition_decimal_long']" +
                ")";
        assertUpdate(createPartitionedTable);

        // Verify the table is correctly created
        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_original");

        // Verify the partition keys are correctly created
        List<String> partitionedBy = ImmutableList.of("partition_bigint", "partition_decimal_long");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        // Create a table using only one LIKE
        @Language("SQL") String createTableSingleLike = "" +
                "CREATE TABLE test_partitioned_table_single_like (" +
                "LIKE test_partitioned_table_original " + likeSuffix +
                ")";
        assertUpdate(createTableSingleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_single_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableLikeExtra = "" +
                "CREATE TABLE test_partitioned_table_like_extra (" +
                "  bigint_col BIGINT" +
                ", double_col DOUBLE" +
                ", LIKE test_partitioned_table_single_like " + likeSuffix +
                ")";
        assertUpdate(createTableLikeExtra);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_like_extra");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "bigint_col", BIGINT);
        assertColumnType(tableMetadata, "double_col", DOUBLE);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableDoubleLike = "" +
                "CREATE TABLE test_partitioned_table_double_like (" +
                "  LIKE test_table_original " +
                ", LIKE test_partitioned_table_like_extra " + likeSuffix +
                ")";
        assertUpdate(createTableDoubleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_double_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        assertUpdate("DROP TABLE test_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_single_like");
        assertUpdate("DROP TABLE test_partitioned_table_like_extra");
        assertUpdate("DROP TABLE test_partitioned_table_double_like");
    }

    @Test
    public void testCreateTableAs()
    {
        testWithAllStorageFormats(this::testCreateTableAs);
    }

    private void testCreateTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", CAST('bar' AS CHAR(10)) _char" +
                ", CAST (1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST ('123.45' as REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        String createTableAs = format("CREATE TABLE test_format_table WITH (format = '%s') AS %s", storageFormat, select);

        assertUpdate(session, createTableAs, 1);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_varchar", createVarcharType(3));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        // assure reader supports basic column reordering and pruning
        assertQuery(session, "SELECT _integer, _varchar, _integer FROM test_format_table", "SELECT 2, 'foo', 2");

        assertQuery(session, "SELECT * FROM test_format_table", select);

        assertUpdate(session, "DROP TABLE test_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_format_table"));
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTableAs);
    }

    private void testCreatePartitionedTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) FROM orders");

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * FROM test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as"));
    }

    @Test
    public void testPropertiesTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_show_properties" +
                " WITH (" +
                "format = 'orc', " +
                "partitioned_by = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_columns = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_fpp = 0.5" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        String queryId = (String) computeScalar("SELECT query_id FROM system.runtime.queries WHERE query LIKE 'CREATE TABLE test_show_properties%'");
        String nodeVersion = (String) computeScalar("SELECT node_version FROM system.runtime.nodes WHERE coordinator");
        assertQuery("SELECT * FROM \"test_show_properties$properties\"",
                "SELECT '" + "ship_priority,order_status" + "','" + "0.5" + "','" + queryId + "','" + nodeVersion + "'");
        assertUpdate("DROP TABLE test_show_properties");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_invalid_column_ordering\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['apple'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableAsInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_as_invalid_column_ordering " +
                "WITH (partitioned_by = ARRAY['SHIP_PRIORITY', 'ORDER_STATUS']) " +
                "AS " +
                "SELECT shippriority AS ship_priority, orderkey AS order_key, orderstatus AS order_status " +
                "FROM tpch.tiny.orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Table contains only partition columns")
    public void testCreateTableOnlyPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_only_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['grape', 'apple', 'orange', 'pear'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition columns .* not present in schema")
    public void testCreateTableNonExistentPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_nonexistent_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['dragonfruit'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: .*")
    public void testCreateTableUnsupportedPartitionType()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type " +
                "(foo bigint, bar ARRAY(varchar)) " +
                "WITH (partitioned_by = ARRAY['bar'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: a")
    public void testCreateTableUnsupportedPartitionTypeAs()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type_as " +
                "WITH (partitioned_by = ARRAY['a']) " +
                "AS " +
                "SELECT 123 x, ARRAY ['foo'] a");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.")
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
    }

    @Test
    public void testEmptyBucketedTable()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testEmptyBucketedTable);
    }

    private void testEmptyBucketedTable(Session session, HiveStorageFormat storageFormat)
    {
        testEmptyBucketedTable(session, storageFormat, true);
        testEmptyBucketedTable(session, storageFormat, false);
    }

    private void testEmptyBucketedTable(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_empty_bucketed_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(bucket_key VARCHAR, col_1 VARCHAR, col2 VARCHAR) " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertNull(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        assertEquals(computeActual("SELECT * from " + tableName).getRowCount(), 0);

        // make sure that we will get one file per bucket regardless of writer count configured
        Session parallelWriter = Session.builder(getParallelWriteSession())
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')", 1);
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testBucketedTable()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testBucketedTable);
    }

    private void testBucketedTable(Session session, HiveStorageFormat storageFormat)
    {
        testBucketedTable(session, storageFormat, true);
        testBucketedTable(session, storageFormat, false);
    }

    private void testBucketedTable(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_bucketed_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t (bucket_key, col_1, col_2)";

        // make sure that we will get one file per bucket regardless of writer count configured
        Session parallelWriter = Session.builder(getParallelWriteSession())
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, createTable, 3);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertNull(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        assertQuery("SELECT * from " + tableName, "VALUES ('a', 'b', 'c'), ('aa', 'bb', 'cc'), ('aaa', 'bbb', 'ccc')");

        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')", 1);
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a', 'b', 'c'), ('aa', 'bb', 'cc'), ('aaa', 'bbb', 'ccc'), ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAsFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testCreatePartitionedBucketedTableAsFewRows);
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat)
    {
        testCreatePartitionedBucketedTableAsFewRows(session, storageFormat, true);
        testCreatePartitionedBucketedTableAsFewRows(session, storageFormat, false);
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_few_rows";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t(bucket_key, col, partition_key)";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                Session.builder(getParallelWriteSession())
                        .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                        .build(),
                createTable,
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) FROM orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAsWithUnionAll()
    {
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_with_union_all";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 0 " +
                "UNION ALL " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 1";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) FROM orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("orderstatus"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey", "custkey2"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * FROM " + tableName, "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * FROM %s WHERE custkey = %d AND custkey2 = %d", tableName, i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders WHERE custkey = %d", i));
        }
    }

    @Test
    public void testCreateInvalidBucketedTable()
    {
        testCreateInvalidBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testCreateInvalidBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_invalid_bucketed_table";

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " (" +
                    "  a BIGINT," +
                    "  b DOUBLE," +
                    "  p VARCHAR" +
                    ") WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'p' ], " +
                    "bucketed_by = ARRAY[ 'a', 'c' ], " +
                    "bucket_count = 11 " +
                    ")");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [c] not present in schema");
        }

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " " +
                    "WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'orderstatus' ], " +
                    "bucketed_by = ARRAY[ 'custkey', 'custkey3' ], " +
                    "bucket_count = 11 " +
                    ") " +
                    "AS " +
                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                    "FROM tpch.tiny.orders");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [custkey3] not present in schema");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedUnionAll()
    {
        assertUpdate("CREATE TABLE test_create_partitioned_union_all (a varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
        assertUpdate("INSERT INTO test_create_partitioned_union_all SELECT 'a', '2013-05-17' UNION ALL SELECT 'b', '2013-05-17'", 2);
        assertUpdate("DROP TABLE test_create_partitioned_union_all");
    }

    @Test
    public void testInsertPartitionedBucketedTableFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testInsertPartitionedBucketedTableFewRows);
    }

    private void testInsertPartitionedBucketedTableFewRows(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table_few_rows";

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  bucket_key varchar," +
                "  col varchar," +
                "  partition_key varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11)");

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                "INSERT INTO " + tableName + " " +
                        "VALUES " +
                        "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                        "  ('aa', 'bb', 'cc'), " +
                        "  ('aaa', 'bbb', 'ccc')",
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE test_insert_partitioned_bucketed_table_few_rows");
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    private void verifyPartitionedBucketedTableAsFewRows(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("partition_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        MaterializedResult actual = computeActual("SELECT * FROM " + tableName);
        MaterializedResult expected = resultBuilder(getSession(), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()))
                .row("a", "b", "c")
                .row("aa", "bb", "cc")
                .row("aaa", "bbb", "ccc")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCastNullToColumnTypes()
    {
        String tableName = "test_cast_null_to_column_types";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  col1 bigint," +
                "  col2 map(bigint, bigint)," +
                "  partition_key varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'partition_key' ] " +
                ")");

        assertUpdate(format("INSERT INTO %s (col1) VALUES (1), (2), (3)", tableName), 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyNonBucketedPartition()
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 0");

        // create an empty partition
        assertUpdate(format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyBucketedPartition()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testCreateEmptyBucketedPartition(storageFormat.getFormat());
        }
    }

    private void testCreateEmptyBucketedPartition(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_empty_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String sql = format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", TPCH_SCHEMA, tableName, orderStatusList.get(i));
            assertUpdate(sql);
            assertQuery(
                    format("SELECT count(*) FROM \"%s$partitions\"", tableName),
                    "SELECT " + (i + 1));

            assertQueryFails(sql, "Partition already exists.*");
        }

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTable()
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s'",
                            orderStatus),
                    format("SELECT count(*) FROM orders WHERE orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void createPartitionedBucketedTable(String tableName, HiveStorageFormat storageFormat)
    {
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");
    }

    @Test
    public void testInsertPartitionedBucketedTableWithUnionAll()
    {
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table_with_union_all";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' AND length(comment) %% 2 = 0 " +
                                    "UNION ALL " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' AND length(comment) %% 2 = 1",
                            orderStatus, orderStatus),
                    format("SELECT count(*) FROM orders WHERE orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertTwiceToSamePartitionedBucket()
    {
        String tableName = "test_insert_twice_to_same_partitioned_bucket";
        createPartitionedBucketedTable(tableName, HiveStorageFormat.RCBINARY);

        String insert = "INSERT INTO " + tableName +
                " VALUES (1, 1, 'first_comment', 'F'), (2, 2, 'second_comment', 'G')";
        assertUpdate(insert, 2);
        assertUpdate(insert, 2);

        assertQuery(
                "SELECT custkey, custkey2, comment, orderstatus FROM " + tableName + " ORDER BY custkey",
                "VALUES (1, 1, 'first_comment', 'F'), (1, 1, 'first_comment', 'F'), (2, 2, 'second_comment', 'G'), (2, 2, 'second_comment', 'G')");
        assertQuery(
                "SELECT custkey, custkey2, comment, orderstatus FROM " + tableName + " WHERE custkey = 1 and custkey2 = 1",
                "VALUES (1, 1, 'first_comment', 'F'), (1, 1, 'first_comment', 'F')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsert()
    {
        testWithAllStorageFormats(this::testInsert);
    }

    private void testInsert(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_format_table " +
                "(" +
                "  _string VARCHAR," +
                "  _varchar VARCHAR(65535)," +
                "  _char CHAR(10)," +
                "  _bigint BIGINT," +
                "  _integer INTEGER," +
                "  _smallint SMALLINT," +
                "  _tinyint TINYINT," +
                "  _real REAL," +
                "  _double DOUBLE," +
                "  _boolean BOOLEAN," +
                "  _decimal_short DECIMAL(3,2)," +
                "  _decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        @Language("SQL") String select = "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", 1 _bigint" +
                ", CAST(42 AS INTEGER) _integer" +
                ", CAST(43 AS SMALLINT) _smallint" +
                ", CAST(44 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (43 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (44 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_insert_format_table " + select, 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table", select);

        assertUpdate(session, "INSERT INTO test_insert_format_table (_tinyint, _smallint, _integer, _bigint, _real, _double) SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4, cast(14.3E0 as REAL), 14.3E0", 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _bigint = 4", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _real = CAST(14.3 as REAL)", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72E0, 3", 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _bigint = 3", "SELECT null, null, null, 3, null, null, null, null, 2.72, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_decimal_short, _decimal_long) SELECT DECIMAL '2.72', DECIMAL '98765432101234567890.0123456789'", 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _decimal_long = DECIMAL '98765432101234567890.0123456789'", "SELECT null, null, null, null, null, null, null, null, null, null, 2.72, 98765432101234567890.0123456789");

        assertUpdate(session, "DROP TABLE test_insert_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_format_table"));
    }

    @Test
    public void testInsertPartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTable);
    }

    private void testInsertPartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY INTEGER," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        String partitionsTable = "\"test_insert_partitioned_table$partitions\"";

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertUpdate(
                session,
                "" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) FROM orders");

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * FROM test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE no_such_column = 1", "line \\S*: Column 'no_such_column' cannot be resolved");
        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE orderkey = 1", "line \\S*: Column 'orderkey' cannot be resolved");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_partitioned_table"));
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTableExistingPartition);
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) FROM orders WHERE orderkey %% 3 = %d", i));
        }

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * FROM " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableOverwriteExistingPartition()
    {
        testInsertPartitionedTableOverwriteExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "OVERWRITE")
                        .build(),
                HiveStorageFormat.ORC);
    }

    private void testInsertPartitionedTableOverwriteExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_overwrite_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) FROM orders WHERE orderkey %% 3 = %d", i));

            // verify the partitions
            List<?> partitions = getPartitions(tableName);
            assertEquals(partitions.size(), 3);

            assertQuery(
                    session,
                    "SELECT * FROM " + tableName,
                    format("SELECT orderkey, comment, orderstatus FROM orders WHERE orderkey %% 3 = %d", i));
        }
        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testNullPartitionValues()
    {
        assertUpdate("" +
                "CREATE TABLE test_null_partition (test VARCHAR, part VARCHAR)\n" +
                "WITH (partitioned_by = ARRAY['part'])");

        assertUpdate("INSERT INTO test_null_partition VALUES ('hello', 'test'), ('world', null)", 2);

        assertQuery(
                "SELECT * FROM test_null_partition",
                "VALUES ('hello', 'test'), ('world', null)");

        assertQuery(
                "SELECT * FROM \"test_null_partition$partitions\"",
                "VALUES 'test', null");

        assertUpdate("DROP TABLE test_null_partition");
    }

    @Test
    public void testPartitionPerScanLimit()
    {
        TestingHiveStorageFormat storageFormat = new TestingHiveStorageFormat(getSession(), HiveStorageFormat.ORC);
        testWithStorageFormat(storageFormat, this::testPartitionPerScanLimit);
    }

    private void testPartitionPerScanLimit(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_partition_per_scan_limit";
        String partitionsTable = "\"" + tableName + "$partitions\"";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part BIGINT" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'part' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("part"));

        // insert 1200 partitions
        for (int i = 0; i < 12; i++) {
            int partStart = i * 100;
            int partEnd = (i + 1) * 100 - 1;

            @Language("SQL") String insertPartitions = "" +
                    "INSERT INTO " + tableName + " " +
                    "SELECT 'bar' foo, part " +
                    "FROM UNNEST(SEQUENCE(" + partStart + ", " + partEnd + ")) AS TMP(part)";

            assertUpdate(session, insertPartitions, 100);
        }

        // we are not constrained by hive.max-partitions-per-scan when listing partitions
        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part > 490 AND part <= 500",
                "VALUES 491, 492, 493, 494, 495, 496, 497, 498, 499, 500");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part < 0",
                "SELECT null WHERE false");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "VALUES " + LongStream.range(0, 1200)
                        .mapToObj(String::valueOf)
                        .collect(joining(",")));

        // verify can query 1000 partitions
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part < 1000",
                "SELECT 1000");

        // verify the rest 200 partitions are successfully inserted
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part >= 1000 AND part < 1200",
                "SELECT 200");

        // verify cannot query more than 1000 partitions
        assertQueryFails(
                session,
                "SELECT * FROM " + tableName + " WHERE part < 1001",
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        // verify cannot query all partitions
        assertQueryFails(
                session,
                "SELECT * FROM " + tableName,
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testShowColumnsFromPartitions()
    {
        String tableName = "test_show_columns_from_partitions";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQuery(
                getSession(),
                "SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                "VALUES ('part1', 'bigint', '', ''), ('part2', 'varchar', '', '')");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"$partitions\"",
                ".*Table '.*\\.tpch\\.\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"orders$partitions\"",
                ".*Table '.*\\.tpch\\.orders\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"blah$partitions\"",
                ".*Table '.*\\.tpch\\.blah\\$partitions' does not exist");
    }

    @Test
    public void testPartitionsTableInvalidAccess()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitions_invalid " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"test_partitions_invalid$partitions$partitions\"",
                ".*Table .*\\.tpch\\.test_partitions_invalid\\$partitions\\$partitions does not exist");

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"non_existent$partitions\"",
                ".*Table .*\\.tpch\\.non_existent\\$partitions does not exist");
    }

    @Test
    public void testInsertUnpartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertUnpartitionedTable);
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_unpartitioned_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "'" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) FROM orders WHERE orderkey %% 3 = %d", i));
        }

        assertQuery(
                session,
                "SELECT * FROM " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * FROM test_delete_unpartitioned");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_delete_unpartitioned"));
    }

    @Test
    public void testMetadataDelete()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  LINE_NUMBER INTEGER," +
                "  LINE_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'LINE_NUMBER', 'LINE_STATUS' ]" +
                ") ";

        assertUpdate(createTable);

        assertUpdate("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) FROM lineitem");

        // Delete returns number of rows deleted, or null if obtaining the number is hard or impossible.
        // Currently, Hive implementation always returns null.
        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='F' AND LINE_NUMBER=CAST(3 AS INTEGER)");

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'F' or linenumber<>3");

        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='O'");

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' AND linenumber<>3");

        try {
            getQueryRunner().execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' AND linenumber<>3");

        assertUpdate("DROP TABLE test_metadata_delete");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, schema, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private Object getHiveTableProperty(String tableName, Function<HiveTableHandle, Object> propertyGetter)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    QualifiedObjectName name = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    TableHandle table = metadata.getTableHandle(transactionSession, name)
                            .orElseThrow(() -> new AssertionError("table not found: " + name));
                    table = metadata.applyFilter(transactionSession, table, Constraint.alwaysTrue())
                            .orElseThrow(() -> new AssertionError("applyFilter did not return a result"))
                            .getHandle();
                    return propertyGetter.apply((HiveTableHandle) table.getConnectorHandle());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        return (List<?>) getHiveTableProperty(tableName, handle -> handle.getPartitions().get());
    }

    private int getBucketCount(String tableName)
    {
        return (int) getHiveTableProperty(tableName, table -> table.getBucketHandle().get().getTableBucketCount());
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar(65535), mango integer, lychee smallint, kiwi tinyint, apple varchar, pineapple varchar(65535))\n" +
                "WITH (partitioned_by = ARRAY['apple', 'pineapple'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM test_show_columns_partition_key");
        Type unboundedVarchar = canonicalizeType(VARCHAR);
        MaterializedResult expected = resultBuilder(getSession(), unboundedVarchar, unboundedVarchar, unboundedVarchar, unboundedVarchar)
                .row("grape", canonicalizeTypeName("bigint"), "", "")
                .row("orange", canonicalizeTypeName("bigint"), "", "")
                .row("pear", canonicalizeTypeName("varchar(65535)"), "", "")
                .row("mango", canonicalizeTypeName("integer"), "", "")
                .row("lychee", canonicalizeTypeName("smallint"), "", "")
                .row("kiwi", canonicalizeTypeName("tinyint"), "", "")
                .row("apple", canonicalizeTypeName("varchar"), "partition key", "")
                .row("pineapple", canonicalizeTypeName("varchar(65535)"), "partition key", "")
                .build();
        assertEquals(actual, expected);
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");

        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[ARRAY[INTEGER'1', INTEGER'2'], NULL, ARRAY[INTEGER'3', INTEGER'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array7", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[ARRAY[SMALLINT'1', SMALLINT'2'], NULL, ARRAY[SMALLINT'3', SMALLINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array8", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array9 AS SELECT ARRAY[ARRAY[TINYINT'1', TINYINT'2'], NULL, ARRAY[TINYINT'3', TINYINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array9", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array10 AS SELECT ARRAY[ARRAY[DECIMAL '3.14']] AS col1, ARRAY[ARRAY[DECIMAL '12345678901234567890.0123456789']] AS col2", 1);
        assertQuery("SELECT col1[1][1] FROM tmp_array10", "SELECT 3.14");
        assertQuery("SELECT col2[1][1] FROM tmp_array10", "SELECT 12345678901234567890.0123456789");

        assertUpdate("CREATE TABLE tmp_array13 AS SELECT ARRAY[ARRAY[REAL'1.234', REAL'2.345'], NULL, ARRAY[REAL'3.456', REAL'4.567']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array13", "SELECT 2.345");
    }

    @Test
    public void testTemporalArrays()
    {
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array12");
    }

    @Test
    public void testMaps()
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[INTEGER'1'], ARRAY[INTEGER'2']) AS col", 1);
        assertQuery("SELECT col[INTEGER'1'] FROM tmp_map2", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY[SMALLINT'1'], ARRAY[SMALLINT'2']) AS col", 1);
        assertQuery("SELECT col[SMALLINT'1'] FROM tmp_map3", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TINYINT'1'], ARRAY[TINYINT'2']) AS col", 1);
        assertQuery("SELECT col[TINYINT'1'] FROM tmp_map4", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map5", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map6", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", 1);
        assertQuery("SELECT col[TRUE] FROM tmp_map7", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map8 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map8");
        assertUpdate("CREATE TABLE tmp_map9 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map9");

        assertUpdate("CREATE TABLE tmp_map10 AS SELECT MAP(ARRAY[DECIMAL '3.14', DECIMAL '12345678901234567890.0123456789'], " +
                "ARRAY[DECIMAL '12345678901234567890.0123456789', DECIMAL '3.0123456789']) AS col", 1);
        assertQuery("SELECT col[DECIMAL '3.14'], col[DECIMAL '12345678901234567890.0123456789'] FROM tmp_map10", "SELECT 12345678901234567890.0123456789, 3.0123456789");

        assertUpdate("CREATE TABLE tmp_map11 AS SELECT MAP(ARRAY[REAL'1.234'], ARRAY[REAL'2.345']) AS col", 1);
        assertQuery("SELECT col[REAL'1.234'] FROM tmp_map11", "SELECT 2.345");

        assertUpdate("CREATE TABLE tmp_map12 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map12", "SELECT 2");
    }

    @Test
    public void testRows()
    {
        assertUpdate("CREATE TABLE tmp_row1 AS SELECT cast(row(CAST(1 as BIGINT), CAST(NULL as BIGINT)) AS row(col0 bigint, col1 bigint)) AS a", 1);
        assertQuery(
                "SELECT a.col0, a.col1 FROM tmp_row1",
                "SELECT 1, cast(null as bigint)");
    }

    @Test
    public void testComplex()
    {
        assertUpdate("CREATE TABLE tmp_complex1 AS SELECT " +
                        "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0E0, 4.0E0]), MAP(ARRAY['c', 'd'], ARRAY[12.0E0, 14.0E0])] AS a",
                1);

        assertQuery(
                "SELECT a[1]['a'], a[2]['d'] FROM tmp_complex1",
                "SELECT 2.0, 14.0");
    }

    @Test
    public void testBucketedCatalog()
    {
        String bucketedCatalog = bucketedSession.getCatalog().get();
        String bucketedSchema = bucketedSession.getSchema().get();

        TableMetadata ordersTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "orders");
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        TableMetadata customerTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "customer");
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);
    }

    @Test
    public void testBucketedExecution()
    {
        assertQuery(bucketedSession, "SELECT count(*) a FROM orders t1 JOIN orders t2 on t1.custkey=t2.custkey");
        assertQuery(bucketedSession, "SELECT count(*) a FROM orders t1 JOIN customer t2 on t1.custkey=t2.custkey", "SELECT count(*) FROM orders");
        assertQuery(bucketedSession, "SELECT count(distinct custkey) FROM orders");

        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "1").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "4").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testScaleWriters()
    {
        try {
            // small table that will only have one writer
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "32MB")
                            .build(),
                    "CREATE TABLE scale_writers_small AS SELECT * FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());

            assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM scale_writers_small").getOnlyValue(), 1L);

            // large table that will scale writers to multiple machines
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "1MB")
                            .build(),
                    "CREATE TABLE scale_writers_large WITH (format = 'RCBINARY') AS SELECT * FROM tpch.sf1.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.sf1.orders").getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_large");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_large");
            assertUpdate("DROP TABLE IF EXISTS scale_writers_small");
        }
    }

    @Test
    public void testTableCommentsTable()
    {
        assertUpdate("CREATE TABLE test_comment (c1 bigint) COMMENT 'foo'");
        String selectTableComment = format("" +
                        "SELECT comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = 'test_comment'",
                getSession().getCatalog().get(),
                getSession().getSchema().get());
        assertQuery(selectTableComment, "SELECT 'foo'");

        assertUpdate("DROP TABLE IF EXISTS test_comment");
    }

    @Test
    public void testShowCreateTable()
    {
        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table");

        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   \"c 2\" varchar,\n" +
                        "   \"c'3\" array(bigint),\n" +
                        "   c4 map(bigint, varchar) COMMENT 'comment test4',\n" +
                        "   c5 double COMMENT 'comment test5'\n)\n" +
                        "COMMENT 'test'\n" +
                        "WITH (\n" +
                        "   bucket_count = 5,\n" +
                        "   bucketed_by = ARRAY['c1','c 2'],\n" +
                        "   format = 'ORC',\n" +
                        "   orc_bloom_filter_columns = ARRAY['c1','c2'],\n" +
                        "   orc_bloom_filter_fpp = 7E-1,\n" +
                        "   partitioned_by = ARRAY['c5'],\n" +
                        "   sorted_by = ARRAY['c1','c 2 DESC']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "\"test_show_create_table'2\"");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table'2\"");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    @Test
    public void testCreateExternalTable()
            throws Exception
    {
        File tempDir = createTempDir();
        File dataFile = new File(tempDir, "test.txt");
        Files.write("hello\u0001world\nbye\u0001world", dataFile, UTF_8);

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_create_external (\n" +
                        "   action varchar,\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   external_location = '%s',\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   textfile_field_separator = U&'\\0001'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertUpdate(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_external");
        assertEquals(actual.getOnlyValue(), createTableSql);

        assertQuery("SELECT action, name FROM test_create_external", "VALUES ('hello', 'world'), ('bye', 'world')");
        assertUpdate("DROP TABLE test_create_external");

        // file should still exist after drop
        assertFile(dataFile);

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateExternalTableTextFileFieldSeparatorEscape()
            throws Exception
    {
        String tableName = "test_create_external_text_file_with_field_separator_and_escape";

        File tempDir = createTempDir();
        File dataFile = new File(tempDir, "test.txt");
        Files.write("HelloEFFWorld\nByeEFFWorld", dataFile, UTF_8);

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   action varchar,\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   external_location = '%s',\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   textfile_field_separator = 'F',\n" +
                        "   textfile_field_separator_escape = 'E'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertUpdate(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_external_text_file_with_field_separator_and_escape");
        assertEquals(actual.getOnlyValue(), createTableSql);

        assertQuery("SELECT action, name FROM test_create_external_text_file_with_field_separator_and_escape", "VALUES ('HelloF', 'World'), ('ByeF', 'World')");
        assertUpdate("DROP TABLE test_create_external_text_file_with_field_separator_and_escape");

        // file should still exist after drop
        assertFile(dataFile);

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCommentTable()
    {
        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");

        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        assertUpdate("COMMENT ON TABLE test_comment_table IS 'new comment'");
        String commentedCreateTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'new comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");
        actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), commentedCreateTableSql);

        assertUpdate("COMMENT ON TABLE test_comment_table IS 'updated comment'");
        commentedCreateTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'updated comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");
        actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), commentedCreateTableSql);

        assertUpdate("COMMENT ON TABLE test_comment_table IS ''");
        commentedCreateTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT ''\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_comment_table");
        actualResult = computeActual("SHOW CREATE TABLE test_comment_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), commentedCreateTableSql);

        assertUpdate("DROP TABLE test_comment_table");
    }

    @Test
    public void testCreateTableWithHeaderAndFooterForTextFile()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_table_skip_header (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_table_skip_header");
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE test_table_skip_header");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_table_skip_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   skip_footer_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        actual = computeActual("SHOW CREATE TABLE test_table_skip_footer");
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE test_table_skip_footer");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_table_skip_header_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'TEXTFILE',\n" +
                        "   skip_footer_line_count = 1,\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        actual = computeActual("SHOW CREATE TABLE test_table_skip_header_footer");
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE test_table_skip_header_footer");
    }

    @Test
    public void testCreateTableWithHeaderAndFooterForCsv()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_header (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        MaterializedResult actual = computeActual("SHOW CREATE TABLE csv_table_skip_header");
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE csv_table_skip_header");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_footer_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        actual = computeActual("SHOW CREATE TABLE csv_table_skip_footer");
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE csv_table_skip_footer");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_header_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_footer_line_count = 1,\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        actual = computeActual("SHOW CREATE TABLE csv_table_skip_header_footer");
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate("DROP TABLE csv_table_skip_header_footer");
    }

    @Test
    public void testInsertTableWithHeaderAndFooterForCsv()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_header (\n" +
                        "   name VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        assertThatThrownBy(() -> assertUpdate(
                format("INSERT INTO %s.%s.csv_table_skip_header VALUES ('name')",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get())))
                .hasMessageMatching("Inserting into Hive table with skip.header.line.count property not supported");

        assertUpdate("DROP TABLE csv_table_skip_header");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_footer (\n" +
                        "   name VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_footer_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        assertThatThrownBy(() -> assertUpdate(
                format("INSERT INTO %s.%s.csv_table_skip_footer VALUES ('name')",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get())))
                .hasMessageMatching("Inserting into Hive table with skip.footer.line.count property not supported");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_header_footer (\n" +
                        "   name VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_footer_line_count = 1,\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        assertThatThrownBy(() -> assertUpdate(
                format("INSERT INTO %s.%s.csv_table_skip_header_footer VALUES ('name')",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get())))
                .hasMessageMatching("Inserting into Hive table with skip.header.line.count property not supported");

        assertUpdate("DROP TABLE csv_table_skip_header_footer");
    }

    @Test
    public void testCreateTableWithInvalidProperties()
    {
        // ORC
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'TEXTFILE', orc_bloom_filter_columns = ARRAY['col1'])"))
                .hasMessageMatching("Cannot specify orc_bloom_filter_columns table property for storage format: TEXTFILE");

        // TEXTFILE
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_header (col1 bigint) WITH (format = 'ORC', skip_header_line_count = 1)"))
                .hasMessageMatching("Cannot specify skip_header_line_count table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_footer (col1 bigint) WITH (format = 'ORC', skip_footer_line_count = 1)"))
                .hasMessageMatching("Cannot specify skip_footer_line_count table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_invalid_skip_header (col1 bigint) WITH (format = 'TEXTFILE', skip_header_line_count = -1)"))
                .hasMessageMatching("Invalid value for skip_header_line_count property: -1");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_invalid_skip_footer (col1 bigint) WITH (format = 'TEXTFILE', skip_footer_line_count = -1)"))
                .hasMessageMatching("Invalid value for skip_footer_line_count property: -1");

        // CSV
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_separator = 'S')"))
                .hasMessageMatching("Cannot specify csv_separator table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_separator = 'SS')"))
                .hasMessageMatching("csv_separator must be a single character string, but was: 'SS'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_quote = 'Q')"))
                .hasMessageMatching("Cannot specify csv_quote table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_quote = 'QQ')"))
                .hasMessageMatching("csv_quote must be a single character string, but was: 'QQ'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'ORC', csv_escape = 'E')"))
                .hasMessageMatching("Cannot specify csv_escape table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_escape = 'EE')"))
                .hasMessageMatching("csv_escape must be a single character string, but was: 'EE'");
    }

    @Test
    public void testPathHiddenColumn()
    {
        testWithAllStorageFormats(this::testPathHiddenColumn);
    }

    private void testPathHiddenColumn(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "CREATE TABLE test_path " +
                "WITH (" +
                "format = '" + storageFormat + "'," +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(session, createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_path"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_path");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(PATH_COLUMN_NAME)) {
                // $path should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_path").size(), 3);

        MaterializedResult results = computeActual(session, format("SELECT *, \"%s\" FROM test_path", PATH_COLUMN_NAME));
        Map<Integer, String> partitionPathMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            String pathName = (String) row.getField(2);
            String parentDirectory = new Path(pathName).getParent().toString();

            assertTrue(pathName.length() > 0);
            assertEquals(col0 % 3, col1);
            if (partitionPathMap.containsKey(col1)) {
                // the rows in the same partition should be in the same partition directory
                assertEquals(partitionPathMap.get(col1), parentDirectory);
            }
            else {
                partitionPathMap.put(col1, parentDirectory);
            }
        }
        assertEquals(partitionPathMap.size(), 3);

        assertUpdate(session, "DROP TABLE test_path");
        assertFalse(getQueryRunner().tableExists(session, "test_path"));
    }

    @Test
    public void testBucketHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_bucket_hidden_column " +
                "WITH (" +
                "bucketed_by = ARRAY['col0']," +
                "bucket_count = 2" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 11), (1, 12), (2, 13), " +
                "(3, 14), (4, 15), (5, 16), " +
                "(6, 17), (7, 18), (8, 19)" +
                " ) t (col0, col1) ";
        assertUpdate(createTable, 9);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_bucket_hidden_column");
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("col0"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 2);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, BUCKET_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(BUCKET_COLUMN_NAME)) {
                // $bucket_number should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getBucketCount("test_bucket_hidden_column"), 2);

        MaterializedResult results = computeActual(format("SELECT *, \"%1$s\" FROM test_bucket_hidden_column WHERE \"%1$s\" = 1",
                BUCKET_COLUMN_NAME));
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            int bucket = (int) row.getField(2);

            assertEquals(col1, col0 + 11);
            assertTrue(col1 % 2 == 0);

            // Because Hive's hash function for integer n is h(n) = n.
            assertEquals(bucket, col0 % 2);
        }
        assertEquals(results.getRowCount(), 4);

        assertUpdate("DROP TABLE test_bucket_hidden_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));
    }

    @Test
    public void testFileSizeHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_file_size " +
                "WITH (" +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_file_size"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_size");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_file_size").size(), 3);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_file_size", FILE_SIZE_COLUMN_NAME));
        Map<Integer, Long> fileSizeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            long fileSize = (Long) row.getField(2);

            assertTrue(fileSize > 0);
            assertEquals(col0 % 3, col1);
            if (fileSizeMap.containsKey(col1)) {
                assertEquals(fileSizeMap.get(col1).longValue(), fileSize);
            }
            else {
                fileSizeMap.put(col1, fileSize);
            }
        }
        assertEquals(fileSizeMap.size(), 3);

        assertUpdate("DROP TABLE test_file_size");
    }

    @Test
    public void testFileModifiedTimeHiddenColumn()
    {
        long testStartTime = Instant.now().toEpochMilli();

        @Language("SQL") String createTable = "CREATE TABLE test_file_modified_time " +
                "WITH (" +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_file_modified_time"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_modified_time");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_file_modified_time").size(), 3);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_file_modified_time", FILE_MODIFIED_TIME_COLUMN_NAME));
        Map<Integer, Long> fileModifiedTimeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            long fileModifiedTime = (Long) row.getField(2);

            assertTrue(fileModifiedTime > (testStartTime - 2_000));
            assertEquals(col0 % 3, col1);
            if (fileModifiedTimeMap.containsKey(col1)) {
                assertEquals(fileModifiedTimeMap.get(col1).longValue(), fileModifiedTime);
            }
            else {
                fileModifiedTimeMap.put(col1, fileModifiedTime);
            }
        }
        assertEquals(fileModifiedTimeMap.size(), 3);

        assertUpdate("DROP TABLE test_file_modified_time");
    }

    @Test
    public void testDeleteAndInsert()
    {
        Session session = getSession();

        // Partition 1 is untouched
        // Partition 2 is altered (dropped and then added back)
        // Partition 3 is added
        // Partition 4 is dropped

        assertUpdate(
                session,
                "CREATE TABLE tmp_delete_insert WITH (partitioned_by=array ['z']) AS " +
                        "SELECT * FROM (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2), (401, 4), (402, 4), (403, 4)) t(a, z)",
                6);

        List<MaterializedRow> expectedBefore = resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(401L, 4L)
                .row(402L, 4L)
                .row(403L, 4L)
                .build()
                .getMaterializedRows();
        List<MaterializedRow> expectedAfter = resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(203L, 2L)
                .row(204L, 2L)
                .row(205L, 2L)
                .row(301L, 2L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        try {
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                    .execute(session, transactionSession -> {
                        assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                        assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                        MaterializedResult actualFromAnotherTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromAnotherTransaction, expectedBefore);
                        MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromCurrentTransaction, expectedAfter);
                        rollback();
                    });
        }
        catch (RollbackException e) {
            // ignore
        }

        MaterializedResult actualAfterRollback = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterRollback, expectedBefore);

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                    assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                    MaterializedResult actualOutOfTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualOutOfTransaction, expectedBefore);
                    MaterializedResult actualInTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualInTransaction, expectedAfter);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expectedAfter);
    }

    @Test
    public void testCreateAndInsert()
    {
        Session session = getSession();

        List<MaterializedRow> expected = resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(301L, 3L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(
                            transactionSession,
                            "CREATE TABLE tmp_create_insert WITH (partitioned_by=array ['z']) AS " +
                                    "SELECT * FROM (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2)) t(a, z)",
                            3);
                    assertUpdate(transactionSession, "INSERT INTO tmp_create_insert VALUES (301, 3), (302, 3)", 2);
                    MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_create_insert");
                    assertEqualsIgnoreOrder(actualFromCurrentTransaction, expected);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_create_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expected);
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column (a bigint COMMENT 'test comment AAA')");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b bigint COMMENT 'test comment BBB'");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN a varchar", ".* Column 'a' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN c bad_type", ".* Unknown type 'bad_type' for column 'c'");
        assertQuery("SHOW COLUMNS FROM test_add_column", "VALUES ('a', 'bigint', '', 'test comment AAA'), ('b', 'bigint', '', 'test comment BBB')");
        assertUpdate("DROP TABLE test_add_column");
    }

    @Test
    public void testRenameColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_rename_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN orderkey TO new_orderkey");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN \"$path\" TO test", ".* Cannot rename hidden column");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertUpdate("DROP TABLE test_rename_column");
    }

    @Test
    public void testDropColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_drop_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$path\"", ".* Cannot drop hidden column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN orderstatus", "Cannot drop partition columns");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN orderkey");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN custkey", "Cannot drop the only non-partition column in a table");
        assertQuery("SELECT * FROM test_drop_column", "SELECT custkey, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_drop_column");
    }

    @Test
    public void testAvroTypeValidation()
    {
        assertQueryFails("CREATE TABLE test_avro_types (x map(bigint, bigint)) WITH (format = 'AVRO')", "Column x has a non-varchar map key, which is not supported by Avro");
        assertQueryFails("CREATE TABLE test_avro_types (x tinyint) WITH (format = 'AVRO')", "Column x is tinyint, which is not supported by Avro. Use integer instead.");
        assertQueryFails("CREATE TABLE test_avro_types (x smallint) WITH (format = 'AVRO')", "Column x is smallint, which is not supported by Avro. Use integer instead.");

        assertQueryFails("CREATE TABLE test_avro_types WITH (format = 'AVRO') AS SELECT cast(42 AS smallint) z", "Column z is smallint, which is not supported by Avro. Use integer instead.");
    }

    @Test
    public void testOrderByChar()
    {
        assertUpdate("CREATE TABLE char_order_by (c_char char(2))");
        assertUpdate("INSERT INTO char_order_by (c_char) VALUES" +
                "(CAST('a' as CHAR(2)))," +
                "(CAST('a\0' as CHAR(2)))," +
                "(CAST('a  ' as CHAR(2)))", 3);

        MaterializedResult actual = computeActual(getSession(),
                "SELECT * FROM char_order_by ORDER BY c_char ASC");

        assertUpdate("DROP TABLE char_order_by");

        MaterializedResult expected = resultBuilder(getSession(), createCharType(2))
                .row("a\0")
                .row("a ")
                .row("a ")
                .build();

        assertEquals(actual, expected);
    }

    /**
     * Tests correctness of comparison of char(x) and varchar pushed down to a table scan as a TupleDomain
     */
    @Test
    public void testPredicatePushDownToTableScan()
    {
        // Test not specific to Hive, but needs a connector supporting table creation

        assertUpdate("CREATE TABLE test_table_with_char (a char(20))");
        try {
            assertUpdate("INSERT INTO test_table_with_char (a) VALUES" +
                    "(cast('aaa' as char(20)))," +
                    "(cast('bbb' as char(20)))," +
                    "(cast('bbc' as char(20)))," +
                    "(cast('bbd' as char(20)))", 4);

            assertQuery(
                    "SELECT a, a <= 'bbc' FROM test_table_with_char",
                    "VALUES (cast('aaa' as char(20)), true), " +
                            "(cast('bbb' as char(20)), true), " +
                            "(cast('bbc' as char(20)), true), " +
                            "(cast('bbd' as char(20)), false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES cast('aaa' as char(20)), " +
                            "cast('bbb' as char(20)), " +
                            "cast('bbc' as char(20))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @Test
    public void testPartitionPruning()
    {
        assertUpdate("CREATE TABLE test_partition_pruning (v bigint, k varchar) WITH (partitioned_by = array['k'])");
        assertUpdate("INSERT INTO test_partition_pruning (v, k) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'e')", 4);

        try {
            String query = "SELECT * FROM test_partition_pruning WHERE k = 'a'";
            assertQuery(query, "VALUES (1, 'a')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("a"), EXACTLY),
                                                            new FormattedMarker(Optional.of("a"), EXACTLY)))))));

            query = "SELECT * FROM test_partition_pruning WHERE k IN ('a', 'b')";
            assertQuery(query, "VALUES (1, 'a'), (2, 'b')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("a"), EXACTLY),
                                                            new FormattedMarker(Optional.of("a"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)))))));

            query = "SELECT * FROM test_partition_pruning WHERE k >= 'b'";
            assertQuery(query, "VALUES (2, 'b'), (3, 'c'), (4, 'e')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("c"), EXACTLY),
                                                            new FormattedMarker(Optional.of("c"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("e"), EXACTLY),
                                                            new FormattedMarker(Optional.of("e"), EXACTLY)))))));

            query = "SELECT * FROM (" +
                    "    SELECT * " +
                    "    FROM test_partition_pruning " +
                    "    WHERE v IN (1, 2, 4) " +
                    ") t " +
                    "WHERE t.k >= 'b'";
            assertQuery(query, "VALUES (2, 'b'), (4, 'e')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR.getTypeSignature(),
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("c"), EXACTLY),
                                                            new FormattedMarker(Optional.of("c"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("e"), EXACTLY),
                                                            new FormattedMarker(Optional.of("e"), EXACTLY)))))));
        }
        finally {
            assertUpdate("DROP TABLE test_partition_pruning");
        }
    }

    @Test
    public void testMismatchedBucketing()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing16\n" +
                            "WITH (bucket_count = 16, bucketed_by = ARRAY['key16']) AS\n" +
                            "SELECT orderkey key16, comment value16 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing32\n" +
                            "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS\n" +
                            "SELECT orderkey key32, comment value32 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketingN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);

            Session withMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "true")
                    .build();
            Session withoutMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "false")
                    .build();

            @Language("SQL") String writeToTableWithMoreBuckets = "CREATE TABLE test_mismatch_bucketing_out32\n" +
                    "WITH (bucket_count = 32, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";
            @Language("SQL") String writeToTableWithFewerBuckets = "CREATE TABLE test_mismatch_bucketing_out8\n" +
                    "WITH (bucket_count = 8, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";

            assertUpdate(withoutMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(4));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out32");

            assertUpdate(withMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders");

            assertUpdate(withMismatchOptimization, writeToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out8", "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing16");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing32");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketingN");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out32");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out8");
        }
    }

    @Test
    public void testGroupedExecution()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_grouped_join1\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join2\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                            "SELECT orderkey key2, comment value2 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join3\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                            "SELECT orderkey key3, comment value3 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join4\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key4_bucket']) AS\n" +
                            "SELECT orderkey key4_bucket, orderkey key4_non_bucket, comment value4 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinDual\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['keyD']) AS\n" +
                            "SELECT orderkey keyD, comment valueD FROM orders CROSS JOIN UNNEST(repeat(NULL, 2))",
                    30000);
            assertUpdate(
                    "CREATE TABLE test_grouped_window\n" +
                            "WITH (bucket_count = 5, bucketed_by = ARRAY['key']) AS\n" +
                            "SELECT custkey key, orderkey value FROM orders WHERE custkey <= 5 ORDER BY orderkey LIMIT 10",
                    10);

            // NOT grouped execution; default
            Session notColocated = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "false")
                    .setSystemProperty(GROUPED_EXECUTION, "false")
                    .build();
            // Co-located JOIN with all groups at once, fixed schedule
            Session colocatedAllGroupsAtOnce = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "false")
                    .build();
            // Co-located JOIN, 1 group per worker at a time, fixed schedule
            Session colocatedOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "false")
                    .build();
            // Co-located JOIN with all groups at once, dynamic schedule
            Session colocatedAllGroupsAtOnceDynamic = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .build();
            // Co-located JOIN, 1 group per worker at a time, dynamic schedule
            Session colocatedOneGroupAtATimeDynamic = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .build();
            // Broadcast JOIN, 1 group per worker at a time
            Session broadcastOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .build();

            // Broadcast JOIN, 1 group per worker at a time, dynamic schedule
            Session broadcastOneGroupAtATimeDynamic = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .build();

            //
            // HASH JOIN
            // =========

            @Language("SQL") String joinThreeBucketedTable =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key2 = key3";
            @Language("SQL") String joinThreeMixedTable =
                    "SELECT key1, value1, key2, value2, keyN, valueN\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_joinN\n" +
                            "ON key2 = keyN";
            @Language("SQL") String expectedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders";
            @Language("SQL") String leftJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM test_grouped_join1\n" +
                            "LEFT JOIN (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "ON key1 = key2";
            @Language("SQL") String rightJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "RIGHT JOIN test_grouped_join1\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedOuterJoinQuery = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN orderkey END, CASE mod(orderkey, 2) WHEN 0 THEN comment END FROM orders";

            assertQuery(notColocated, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(notColocated, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocated, rightJoinBucketedTable, expectedOuterJoinQuery);

            assertQuery(colocatedAllGroupsAtOnce, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnceDynamic, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));

            assertQuery(colocatedAllGroupsAtOnce, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));

            //
            // CROSS JOIN and HASH JOIN mixed
            // ==============================

            @Language("SQL") String crossJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "CROSS JOIN (SELECT * FROM test_grouped_join3 WHERE key3 <= 3)";
            @Language("SQL") String expectedCrossJoinQuery =
                    "SELECT key1, value1, key1, value1, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT orderkey key1, comment value1 FROM orders)\n" +
                            "CROSS JOIN\n" +
                            "  (SELECT orderkey key3, comment value3 FROM orders WHERE orderkey <= 3)";
            assertQuery(notColocated, crossJoin, expectedCrossJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));

            //
            // Bucketed and unbucketed HASH JOIN mixed
            // =======================================
            @Language("SQL") String bucketedAndUnbucketedJoin =
                    "SELECT key1, value1, keyN, valueN, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  test_grouped_join1\n" +
                            "JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  JOIN test_grouped_join2\n" +
                            "  ON keyN = key2\n" +
                            ")\n" +
                            "ON key1 = keyN\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key1 = key3";
            @Language("SQL") String expectedBucketedAndUnbucketedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment, orderkey, comment FROM orders";
            assertQuery(notColocated, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));

            //
            // UNION ALL / GROUP BY
            // ====================

            @Language("SQL") String groupBySingleBucketed =
                    "SELECT\n" +
                            "  keyD,\n" +
                            "  count(valueD)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedSingleGroupByQuery = "SELECT orderkey, 2 FROM orders";
            @Language("SQL") String groupByOfUnionBucketed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(value3)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL value3\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL value3\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT key3 key, NULL value1, NULL value2, value3\n" +
                            "  FROM test_grouped_join3\n" +
                            "  WHERE key3 % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String groupByOfUnionMixed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(valueN)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL valueN\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL valueN\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, NULL value1, NULL value2, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  WHERE keyN % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String expectedGroupByOfUnion = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN comment END, CASE mod(orderkey, 3) WHEN 0 THEN comment END FROM orders";
            // In this case:
            // * left side can take advantage of bucketed execution
            // * right side does not have the necessary organization to allow its parent to take advantage of bucketed execution
            // In this scenario, we give up bucketed execution altogether. This can potentially be improved.
            //
            //       AGG(key)
            //           |
            //       UNION ALL
            //      /         \
            //  AGG(key)  Scan (not bucketed)
            //     |
            // Scan (bucketed on key)
            @Language("SQL") String groupByOfUnionOfGroupByMixed =
                    "SELECT\n" +
                            "  key, sum(cnt) cnt\n" +
                            "FROM (\n" +
                            "  SELECT keyD key, count(valueD) cnt\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, 1 cnt\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "group by key";
            @Language("SQL") String expectedGroupByOfUnionOfGroupBy = "SELECT orderkey, 3 FROM orders";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionMixed, expectedGroupByOfUnion, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionOfGroupByMixed, expectedGroupByOfUnionOfGroupBy, assertRemoteExchangesCount(2));

            //
            // GROUP BY and JOIN mixed
            // ========================
            @Language("SQL") String joinGroupedWithGrouped =
                    "SELECT key1, count1, count2\n" +
                            "FROM (\n" +
                            "  SELECT keyD key1, count(valueD) count1\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD key2, count(valueD) count2\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedJoinGroupedWithGrouped = "SELECT orderkey, 2, 2 FROM orders";
            @Language("SQL") String joinGroupedWithUngrouped =
                    "SELECT keyD, countD, valueN\n" +
                            "FROM (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "ON keyD = keyN";
            @Language("SQL") String expectedJoinGroupedWithUngrouped = "SELECT orderkey, 2, comment FROM orders";
            @Language("SQL") String joinUngroupedWithGrouped =
                    "SELECT keyN, valueN, countD\n" +
                            "FROM (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON keyN = keyD";
            @Language("SQL") String expectedJoinUngroupedWithGrouped = "SELECT orderkey, comment, 2 FROM orders";
            @Language("SQL") String groupOnJoinResult =
                    "SELECT keyD, count(valueD), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON keyD=keyN\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedGroupOnJoinResult = "SELECT orderkey, 2, 2 FROM orders";

            @Language("SQL") String groupOnUngroupedJoinResult =
                    "SELECT key4_bucket, count(value4), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_join4\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON key4_non_bucket=keyN\n" +
                            "GROUP BY key4_bucket";
            @Language("SQL") String expectedGroupOnUngroupedJoinResult = "SELECT orderkey, count(*), count(*) FROM orders group by orderkey";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));

            assertQuery(broadcastOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(broadcastOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));
            assertQuery(broadcastOneGroupAtATimeDynamic, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, joinUngroupedWithGrouped, expectedJoinUngroupedWithGrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(4));

            //
            // Outer JOIN (that involves LookupOuterOperator)
            // ==============================================

            // Chain on the probe side to test duplicating OperatorFactory
            @Language("SQL") String chainedOuterJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT * FROM test_grouped_join1 WHERE mod(key1, 2) = 0)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_join2 WHERE mod(key2, 3) = 0)\n" +
                            "ON key1 = key2\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 WHERE mod(key3, 5) = 0)\n" +
                            "ON key2 = key3";
            // Probe is grouped execution, but build is not
            @Language("SQL") String sharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 WHERE mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN WHERE mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN";
            // The preceding test case, which then feeds into another join
            @Language("SQL") String chainedSharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 WHERE mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN WHERE mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 WHERE mod(key3, 5) = 0)\n" +
                            "ON keyN = key3";
            @Language("SQL") String expectedChainedOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0 OR mod(orderkey, 5) = 0";
            @Language("SQL") String expectedSharedBuildOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN comment END,\n" +
                    "  orderkey,\n" +
                    "  comment\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0";

            assertQuery(notColocated, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(notColocated, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));

            //
            // Window function
            // ===============
            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, count(*) OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, n FROM (SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) AS n FROM test_grouped_window) WHERE n <= 2",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            //
            // Filter out all or majority of splits
            // ====================================
            @Language("SQL") String noSplits =
                    "SELECT key1, arbitrary(value1)\n" +
                            "FROM test_grouped_join1\n" +
                            "WHERE \"$bucket\" < 0\n" +
                            "GROUP BY key1";
            @Language("SQL") String joinMismatchedBuckets =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join1\n" +
                            "  WHERE \"$bucket\"=1\n" +
                            ")\n" +
                            "FULL OUTER JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE \"$bucket\"=11\n" +
                            ")\n" +
                            "ON key1=key2";
            @Language("SQL") String expectedNoSplits = "SELECT 1, 'a' WHERE FALSE";
            @Language("SQL") String expectedJoinMismatchedBuckets = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 13) IN (1, 11)";

            assertQuery(notColocated, noSplits, expectedNoSplits);
            assertQuery(colocatedAllGroupsAtOnce, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(notColocated, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
            assertQuery(colocatedAllGroupsAtOnce, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join1");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join2");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join3");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join4");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinN");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinDual");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_window");
        }
    }

    private Consumer<Plan> assertRemoteExchangesCount(int expectedRemoteExchangesCount)
    {
        return plan ->
        {
            int actualRemoteExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == ExchangeNode.Scope.REMOTE)
                    .findAll()
                    .size();
            if (actualRemoteExchangesCount != expectedRemoteExchangesCount) {
                Session session = getSession();
                Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();
                String formattedPlan = textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, StatsAndCosts.empty(), session, 0, false);
                throw new AssertionError(format(
                        "Expected [\n%s\n] remote exchanges but found [\n%s\n] remote exchanges. Actual plan is [\n\n%s\n]",
                        expectedRemoteExchangesCount,
                        actualRemoteExchangesCount,
                        formattedPlan));
            }
        };
    }

    @Test
    public void testRcTextCharDecoding()
    {
        assertUpdate("CREATE TABLE test_table_with_char_rc WITH (format = 'RCTEXT') AS SELECT CAST('khaki' AS CHAR(7)) char_column", 1);
        try {
            assertQuery(
                    "SELECT * FROM test_table_with_char_rc WHERE char_column = 'khaki  '",
                    "VALUES (CAST('khaki' AS CHAR(7)))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char_rc");
        }
    }

    @Test
    public void testInvalidPartitionValue()
    {
        assertUpdate("CREATE TABLE invalid_partition_value (a int, b varchar) WITH (partitioned_by = ARRAY['b'])");
        assertQueryFails(
                "INSERT INTO invalid_partition_value VALUES (4, 'test' || chr(13))",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: 74 65 73 74 0D\\E");
        assertUpdate("DROP TABLE invalid_partition_value");

        assertQueryFails(
                "CREATE TABLE invalid_partition_value (a, b) WITH (partitioned_by = ARRAY['b']) AS SELECT 4, chr(9731)",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: E2 98 83\\E");
    }

    @Test
    public void testShowColumnMetadata()
    {
        String tableName = "test_show_column_table";

        @Language("SQL") String createTable = "CREATE TABLE " + tableName + " (a bigint, b varchar, c double)";

        Session testSession = testSessionBuilder()
                .setIdentity(Identity.ofUser("test_access_owner"))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        assertUpdate(createTable);

        // verify showing columns over a table requires SELECT privileges for the table
        assertAccessAllowed("SHOW COLUMNS FROM " + tableName);
        assertAccessDenied(testSession,
                "SHOW COLUMNS FROM " + tableName,
                "Cannot show columns of table .*." + tableName + ".*",
                privilege(tableName, SELECT_COLUMN));

        @Language("SQL") String getColumnsSql = "" +
                "SELECT lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_name = '" + tableName + "'";
        assertEquals(computeActual(getColumnsSql).getOnlyColumnAsSet(), ImmutableSet.of("a", "b", "c"));

        // verify with no SELECT privileges on table, querying information_schema will return empty columns
        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().deny(privilege(tableName, SELECT_COLUMN));
                assertQueryReturnsEmptyResult(testSession, getColumnsSql);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowTablePrivileges()
    {
        try {
            assertUpdate("CREATE SCHEMA bar");
            assertUpdate("CREATE TABLE bar.one(t integer)");
            assertUpdate("CREATE TABLE bar.two(t integer)");
            assertUpdate("CREATE VIEW bar.three AS SELECT t FROM bar.one");
            assertUpdate("CREATE SCHEMA foo"); // `foo.two` does not exist. Make sure this doesn't incorrectly show up in listing.

            computeActual("SELECT * FROM information_schema.table_privileges"); // must not fail
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_schema = 'bar'",
                    "VALUES " +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'one', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'one', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'one', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'one', 'UPDATE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'UPDATE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'UPDATE', 'YES', null)");
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_schema = 'bar' AND table_name = 'two'",
                    "VALUES " +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'UPDATE', 'YES', null)");
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_name = 'two'",
                    "VALUES " +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'two', 'UPDATE', 'YES', null)");
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_name = 'three'",
                    "VALUES " +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'admin', 'USER', 'hive', 'bar', 'three', 'UPDATE', 'YES', null)");
        }
        finally {
            computeActual("DROP SCHEMA IF EXISTS foo");
            computeActual("DROP VIEW IF EXISTS bar.three");
            computeActual("DROP TABLE IF EXISTS bar.two");
            computeActual("DROP TABLE IF EXISTS bar.one");
            computeActual("DROP SCHEMA IF EXISTS bar");
        }
    }

    @Test
    public void testCurrentUserInView()
    {
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");
        String testAccountsUnqualifiedName = "test_accounts";
        String testAccountsViewUnqualifiedName = "test_accounts_view";
        String testAccountsViewFullyQualifiedName = format("%s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), testAccountsViewUnqualifiedName);
        assertUpdate(format("CREATE TABLE %s AS SELECT user_name, account_name" +
                "  FROM (VALUES ('user1', 'account1'), ('user2', 'account2'))" +
                "  t (user_name, account_name)", testAccountsUnqualifiedName), 2);
        assertUpdate(format("CREATE VIEW %s AS SELECT account_name FROM test_accounts WHERE user_name = CURRENT_USER", testAccountsViewUnqualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user1", testAccountsViewFullyQualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user2", testAccountsViewFullyQualifiedName));

        Session user1 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(Identity.forUser("user1").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        Session user2 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(Identity.forUser("user2").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        assertQuery(user1, "SELECT account_name FROM test_accounts_view", "VALUES 'account1'");
        assertQuery(user2, "SELECT account_name FROM test_accounts_view", "VALUES 'account2'");
        assertUpdate("DROP VIEW test_accounts_view");
        assertUpdate("DROP TABLE test_accounts");
    }

    @Test
    public void testCollectColumnStatisticsOnCreateTable()
    {
        String tableName = "test_collect_column_statistics_on_create_table";
        assertUpdate(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ") " +
                "AS " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), CAST('bcd1' AS VARBINARY), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), CAST('bcd2' AS VARBINARY), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), CAST('dcb1' AS VARBINARY), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), CAST('dcb2' AS VARBINARY), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCollectColumnStatisticsOnInsert()
    {
        String tableName = "test_collect_column_statistics_on_insert";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_boolean BOOLEAN, " +
                "   c_bigint BIGINT, " +
                "   c_double DOUBLE, " +
                "   c_timestamp TIMESTAMP, " +
                "   c_varchar VARCHAR, " +
                "   c_varbinary VARBINARY, " +
                "   p_varchar VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), CAST('bcd1' AS VARBINARY), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), CAST('bcd2' AS VARBINARY), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), CAST('dcb1' AS VARBINARY), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), CAST('dcb2' AS VARBINARY), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        assertQuery(
                "SELECT * FROM system.metadata.analyze_properties WHERE catalog_name = 'hive'",
                "SELECT 'hive', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'");
    }

    @Test
    public void testAnalyzeEmptyTable()
    {
        String tableName = "test_analyze_empty_table";
        assertUpdate(format("CREATE TABLE %s (c_bigint BIGINT, c_varchar VARCHAR(2))", tableName));
        assertUpdate("ANALYZE " + tableName, 0);
    }

    @Test
    public void testInvalidAnalyzePartitionedTable()
    {
        String tableName = "test_invalid_analyze_partitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, format(".*Table 'hive.tpch.%s' does not exist.*", tableName));

        createPartitionedTableForAnalyzeTest(tableName);

        // Test invalid property
        assertQueryFails(format("ANALYZE %s WITH (error = 1)", tableName), ".*'hive' does not support analyze property 'error'.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = 1)", tableName), "\\QInvalid value for analyze property 'partitions': Cannot convert [1] to array(array(varchar))\\E");
        assertQueryFails(format("ANALYZE %s WITH (partitions = NULL)", tableName), "\\QInvalid value for analyze property 'partitions': Cannot convert [null] to array(array(varchar))\\E");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[NULL])", tableName), ".*Invalid null value in analyze partitions property.*");

        // Test non-existed partition
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10']])", tableName), ".*Partition no longer exists.*");

        // Test partition schema mismatch
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4']])", tableName), "Partition value count does not match partition column count");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10', 'error']])", tableName), "Partition value count does not match partition column count");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testInvalidAnalyzeUnpartitionedTable()
    {
        String tableName = "test_invalid_analyze_unpartitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, ".*Table.*does not exist.*");

        createUnpartitionedTableForAnalyzeTest(tableName);

        // Test partition properties on unpartitioned table
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), "Partition list provided but table is not partitioned");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1']])", tableName), "Partition list provided but table is not partitioned");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        String tableName = "test_analyze_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before running analyze
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 0.0, 0.0, null, null, null), " +
                        "('c_double', null, 0.0, 0.0, null, null, null), " +
                        "('c_timestamp', null, 0.0, 0.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('c_varbinary', 0.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 0.0, 0.0, null, null, null), " +
                        "('c_double', null, 0.0, 0.0, null, null, null), " +
                        "('c_timestamp', null, 0.0, 0.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('c_varbinary', 0.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before running analyze
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Drop the unpartitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    protected void createPartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, true);
    }

    protected void createUnpartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, false);
    }

    private void createTableForAnalyzeTest(String tableName, boolean partitioned)
    {
        Session defaultSession = getSession();

        // Disable column statistics collection when creating the table
        Session disableColumnStatsSession = Session.builder(defaultSession)
                .setCatalogSessionProperty(defaultSession.getCatalog().get(), "collect_column_statistics_on_write", "false")
                .build();

        assertUpdate(
                disableColumnStatsSession,
                "" +
                        "CREATE TABLE " +
                        tableName +
                        (partitioned ? " WITH (partitioned_by = ARRAY['p_varchar', 'p_bigint'])\n" : " ") +
                        "AS " +
                        "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint " +
                        "FROM ( " +
                        "  VALUES " +
                        // p_varchar = 'p1', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', 'abc1', X'bcd1', 'p1', BIGINT '7'), " +
                        "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', 'abc2', X'bcd2', 'p1', BIGINT '7'), " +
                        // p_varchar = 'p2', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', 'cba1', X'dcb1', 'p2', BIGINT '7'), " +
                        "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', 'cba2', X'dcb2', 'p2', BIGINT '7'), " +
                        // p_varchar = 'p3', p_bigint = BIGINT '8'
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (true, BIGINT '3', DOUBLE '4.4', TIMESTAMP '2012-10-10 01:00', 'bca1', X'cdb1', 'p3', BIGINT '8'), " +
                        "    (false, BIGINT '2', DOUBLE '3.4', TIMESTAMP '2012-10-10 00:00', 'bca2', X'cdb2', 'p3', BIGINT '8'), " +
                        // p_varchar = NULL, p_bigint = NULL
                        "    (false, BIGINT '7', DOUBLE '7.7', TIMESTAMP '1977-07-07 07:07', 'efa1', X'efa1', NULL, NULL), " +
                        "    (false, BIGINT '6', DOUBLE '6.7', TIMESTAMP '1977-07-07 07:06', 'efa2', X'efa2', NULL, NULL), " +
                        "    (false, BIGINT '5', DOUBLE '5.7', TIMESTAMP '1977-07-07 07:05', 'efa3', X'efa3', NULL, NULL), " +
                        "    (false, BIGINT '4', DOUBLE '4.7', TIMESTAMP '1977-07-07 07:04', 'efa4', X'efa4', NULL, NULL) " +
                        ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint)", 16);

        if (partitioned) {
            // Create empty partitions
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e1", "9"));
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e2", "9"));
        }
    }

    @Test
    public void testInsertMultipleColumnsFromSameChannel()
    {
        String tableName = "test_insert_multiple_columns_same_channel";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_bigint_1 BIGINT, " +
                "   c_bigint_2 BIGINT, " +
                "   p_varchar_1 VARCHAR, " +
                "   p_varchar_2 VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar_1', 'p_varchar_2'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT 1 c_bigint_1, 1 c_bigint_2, '2' p_varchar_1, '2' p_varchar_2 ", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = '2' AND p_varchar_2 = '2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '1', '1'), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '1', '1'), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null)");

        assertUpdate(format("" +
                "INSERT INTO %s (c_bigint_1, c_bigint_2, p_varchar_1, p_varchar_2) " +
                "SELECT orderkey, orderkey, orderstatus, orderstatus " +
                "FROM orders " +
                "WHERE orderstatus='O' AND orderkey = 15008", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = 'O' AND p_varchar_2 = 'O')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '15008', '15008'), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '15008', '15008'), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCreateAvroTableWithSchemaUrl()
            throws Exception
    {
        String tableName = "test_create_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        String createTableSql = getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath());
        String expectedShowCreateTable = getAvroCreateTableSql(tableName, schemaFile.toURI().toString());

        assertUpdate(createTableSql);

        try {
            MaterializedResult actual = computeActual(format("SHOW CREATE TABLE %s", tableName));
            assertEquals(actual.getOnlyValue(), expectedShowCreateTable);
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    @Test
    public void testAlterAvroTableWithSchemaUrl()
            throws Exception
    {
        testAlterAvroTableWithSchemaUrl(true, true, true);
    }

    protected void testAlterAvroTableWithSchemaUrl(boolean renameColumn, boolean addColumn, boolean dropColumn)
            throws Exception
    {
        String tableName = "test_alter_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        assertUpdate(getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath()));

        try {
            if (renameColumn) {
                assertQueryFails(format("ALTER TABLE %s RENAME COLUMN dummy_col TO new_dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (addColumn) {
                assertQueryFails(format("ALTER TABLE %s ADD COLUMN new_dummy_col VARCHAR", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (dropColumn) {
                assertQueryFails(format("ALTER TABLE %s DROP COLUMN dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    private String getAvroCreateTableSql(String tableName, String schemaFile)
    {
        return format("CREATE TABLE %s.%s.%s (\n" +
                        "   dummy_col varchar,\n" +
                        "   another_dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = '%s',\n" +
                        "   format = 'AVRO'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                schemaFile);
    }

    private static File createAvroSchemaFile()
            throws Exception
    {
        File schemaFile = File.createTempFile("avro_single_column-", ".avsc");
        String schema = "{\n" +
                "  \"namespace\": \"io.prestosql.test\",\n" +
                "  \"name\": \"single_column\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\":\"string_col\", \"type\":\"string\" }\n" +
                "]}";
        asCharSink(schemaFile, UTF_8).write(schema);
        return schemaFile;
    }

    @Test
    public void testCreateOrcTableWithSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_orc (\n" +
                        "   dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = 'dummy.avsc',\n" +
                        "   format = 'ORC'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertQueryFails(createTableSql, "Cannot specify avro_schema_url table property for storage format: ORC");
    }

    @Test
    public void testCtasFailsWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String ctasSqlWithoutData = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT 'dummy_value' as dummy_col WITH NO DATA";

        assertQueryFails(ctasSqlWithoutData, "CREATE TABLE AS not supported when Avro schema url is set");

        @Language("SQL") String ctasSql = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT * FROM (VALUES('a')) t (a)";

        assertQueryFails(ctasSql, "CREATE TABLE AS not supported when Avro schema url is set");
    }

    @Test
    public void testBucketedTablesFailWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      bucket_count = 2, bucketed_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing/Partitioning columns not supported when Avro schema url is set");
    }

    @Test
    public void testPartitionedTablesFailWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      partitioned_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing/Partitioning columns not supported when Avro schema url is set");
    }

    @Test
    public void testPrunePartitionFailure()
    {
        assertUpdate("CREATE TABLE test_prune_failure\n" +
                "WITH (partitioned_by = ARRAY['p']) AS\n" +
                "SELECT 123 x, 'abc' p", 1);

        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM test_prune_failure\n" +
                "WHERE x < 0 AND cast(p AS int) > 0");

        assertUpdate("DROP TABLE test_prune_failure");
    }

    @Test
    public void testTemporaryStagingDirectorySessionProperties()
    {
        String tableName = "test_temporary_staging_directory_session_properties";
        assertUpdate(format("CREATE TABLE %s(i int)", tableName));

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "false")
                .build();

        HiveInsertTableHandle hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());

        session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "true")
                .setCatalogSessionProperty("hive", "temporary_staging_directory_path", "/tmp/custom/temporary-${USER}")
                .build();

        hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertNotEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());
        assertTrue(hiveInsertTableHandle.getLocationHandle().getWritePath().toString().startsWith("file:/tmp/custom/temporary-"));

        assertUpdate("DROP TABLE " + tableName);
    }

    private HiveInsertTableHandle getHiveInsertTableHandle(Session session, String tableName)
    {
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    QualifiedObjectName objectName = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    Optional<TableHandle> handle = metadata.getTableHandle(transactionSession, objectName);
                    InsertTableHandle insertTableHandle = metadata.beginInsert(transactionSession, handle.get());
                    HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle.getConnectorHandle();

                    metadata.finishInsert(transactionSession, insertTableHandle, ImmutableList.of(), ImmutableList.of());
                    return hiveInsertTableHandle;
                });
    }

    @Test
    public void testSelectWithNoColumns()
    {
        testWithAllStorageFormats(this::testSelectWithNoColumns);
    }

    private void testSelectWithNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_select_with_no_columns";
        @Language("SQL") String createTable = format(
                "CREATE TABLE %s (col0) WITH (format = '%s') AS VALUES 5, 6, 7",
                tableName,
                storageFormat);
        assertUpdate(session, createTable, 3);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertQuery("SELECT 1 FROM " + tableName, "VALUES 1, 1, 1");
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnsupportedCsvTable()
    {
        assertQueryFails(
                "CREATE TABLE create_unsupported_csv(i INT, bound VARCHAR(10), unbound VARCHAR, dummy VARCHAR) WITH (format = 'CSV')",
                "\\QHive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: i integer, bound varchar(10)\\E");
    }

    private Session getParallelWriteSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_writer_count", "4")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }

    private Type canonicalizeType(Type type)
    {
        HiveType hiveType = HiveType.toHiveType(typeTranslator, type);
        return TYPE_MANAGER.getType(hiveType.getTypeSignature());
    }

    private String canonicalizeTypeName(String type)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(type);
        return canonicalizeType(TYPE_MANAGER.getType(typeSignature)).toString();
    }

    private void assertColumnType(TableMetadata tableMetadata, String columnName, Type expectedType)
    {
        assertEquals(tableMetadata.getColumn(columnName).getType(), canonicalizeType(expectedType));
    }

    private void assertConstraints(@Language("SQL") String query, Set<ColumnConstraint> expected)
    {
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        Set<ColumnConstraint> constraints = jsonCodec(IoPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))
                .getInputTableColumnInfos().stream()
                .findFirst().get()
                .getColumnConstraints();

        assertEquals(constraints, expected);
    }

    private void verifyPartition(boolean hasPartition, TableMetadata tableMetadata, List<String> partitionKeys)
    {
        Object partitionByProperty = tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY);
        if (hasPartition) {
            assertEquals(partitionByProperty, partitionKeys);
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                boolean partitionKey = partitionKeys.contains(columnMetadata.getName());
                assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
            }
        }
        else {
            assertNull(partitionByProperty);
        }
    }

    private void rollback()
    {
        throw new RollbackException();
    }

    private static class RollbackException
            extends RuntimeException
    {
    }

    private static ConnectorSession getConnectorSession(Session session)
    {
        return session.toConnectorSession(new CatalogName(session.getCatalog().get()));
    }

    private void testWithAllStorageFormats(BiConsumer<Session, HiveStorageFormat> test)
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testWithStorageFormat(storageFormat, test);
        }
    }

    private static void testWithStorageFormat(TestingHiveStorageFormat storageFormat, BiConsumer<Session, HiveStorageFormat> test)
    {
        requireNonNull(storageFormat, "storageFormat is null");
        requireNonNull(test, "test is null");
        Session session = storageFormat.getSession();
        try {
            test.accept(session, storageFormat.getFormat());
        }
        catch (Exception | AssertionError e) {
            fail(format("Failure for format %s with properties %s", storageFormat.getFormat(), session.getConnectorProperties()), e);
        }
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        Session session = getSession();
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            if (hiveStorageFormat == HiveStorageFormat.CSV) {
                // CSV supports only unbounded VARCHAR type
                continue;
            }
            formats.add(new TestingHiveStorageFormat(session, hiveStorageFormat));
        }
        return formats.build();
    }

    private static class TestingHiveStorageFormat
    {
        private final Session session;
        private final HiveStorageFormat format;

        TestingHiveStorageFormat(Session session, HiveStorageFormat format)
        {
            this.session = requireNonNull(session, "session is null");
            this.format = requireNonNull(format, "format is null");
        }

        public Session getSession()
        {
            return session;
        }

        public HiveStorageFormat getFormat()
        {
            return format;
        }
    }

    private static class TypeAndEstimate
    {
        public final Type type;
        public final EstimatedStatsAndCost estimate;

        public TypeAndEstimate(Type type, EstimatedStatsAndCost estimate)
        {
            this.type = requireNonNull(type, "type is null");
            this.estimate = requireNonNull(estimate, "estimate is null");
        }
    }
}
