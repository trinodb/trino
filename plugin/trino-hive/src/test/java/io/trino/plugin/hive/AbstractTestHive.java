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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.azure.HiveAzureConfig;
import io.trino.plugin.hive.azure.TrinoAzureConfigurationInitializer;
import io.trino.plugin.hive.fs.CachingDirectoryLister;
import io.trino.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.MetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.TestingMetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.rcfile.RcFilePageSource;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.security.SqlStandardAccessControlMetadata;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.sql.gen.JoinCompiler;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import io.trino.type.BlockTypeOperators;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.difference;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.COMMIT;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_APPEND_PAGE;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_BEGIN_INSERT;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_DELETE;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_FINISH_INSERT;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_SINK_FINISH;
import static io.trino.plugin.hive.AbstractTestHive.TransactionDeleteInsertTestTag.ROLLBACK_RIGHT_AWAY;
import static io.trino.plugin.hive.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.plugin.hive.HiveBasicStatistics.createZeroStatistics;
import static io.trino.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.bucketColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.trino.plugin.hive.HiveSessionProperties.getTemporaryStagingDirectoryPath;
import static io.trino.plugin.hive.HiveSessionProperties.isTemporaryStagingDirectoryEnabled;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.HiveStorageFormat.JSON;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveStorageFormat.RCBINARY;
import static io.trino.plugin.hive.HiveStorageFormat.RCTEXT;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveTableRedirectionsProvider.NO_REDIRECTIONS;
import static io.trino.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.arrayType;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProviders;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.trino.plugin.hive.HiveTestUtils.getTypes;
import static io.trino.plugin.hive.HiveTestUtils.mapType;
import static io.trino.plugin.hive.HiveTestUtils.rowType;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.SortingColumn.Order.ASCENDING;
import static io.trino.plugin.hive.metastore.SortingColumn.Order.DESCENDING;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.cachingHiveMetastore;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_NAME;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_VALUE;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static io.trino.plugin.hive.util.HiveUtil.columnExtraInfo;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.plugin.hive.util.HiveWriteUtils.createDirectory;
import static io.trino.plugin.hive.util.HiveWriteUtils.getTableDefaultLocation;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

// staging directory is shared mutable state
@Test(singleThreaded = true)
public abstract class AbstractTestHive
{
    private static final Logger log = Logger.get(AbstractTestHive.class);

    protected static final String TEMPORARY_TABLE_PREFIX = "tmp_trino_test_";

    protected static final String INVALID_DATABASE = "totally_invalid_database_name";
    protected static final String INVALID_TABLE = "totally_invalid_table_name";
    protected static final String INVALID_COLUMN = "totally_invalid_column_name";

    protected static final String TEST_SERVER_VERSION = "test_version";

    private static final Type ARRAY_TYPE = arrayType(createUnboundedVarcharType());
    private static final Type MAP_TYPE = mapType(createUnboundedVarcharType(), BIGINT);
    private static final Type ROW_TYPE = rowType(ImmutableList.of(
            new NamedTypeSignature(Optional.of(new RowFieldName("f_string")), createUnboundedVarcharType().getTypeSignature()),
            new NamedTypeSignature(Optional.of(new RowFieldName("f_bigint")), BIGINT.getTypeSignature()),
            new NamedTypeSignature(Optional.of(new RowFieldName("f_boolean")), BOOLEAN.getTypeSignature())));

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("id", BIGINT))
            .add(new ColumnMetadata("t_string", createUnboundedVarcharType()))
            .add(new ColumnMetadata("t_tinyint", TINYINT))
            .add(new ColumnMetadata("t_smallint", SMALLINT))
            .add(new ColumnMetadata("t_integer", INTEGER))
            .add(new ColumnMetadata("t_bigint", BIGINT))
            .add(new ColumnMetadata("t_float", REAL))
            .add(new ColumnMetadata("t_double", DOUBLE))
            .add(new ColumnMetadata("t_boolean", BOOLEAN))
            .add(new ColumnMetadata("t_array", ARRAY_TYPE))
            .add(new ColumnMetadata("t_map", MAP_TYPE))
            .add(new ColumnMetadata("t_row", ROW_TYPE))
            .build();

    private static final MaterializedResult CREATE_TABLE_DATA =
            MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE)
                    .row(1L, "hello", (byte) 45, (short) 345, 234, 123L, -754.1985f, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1L, true))
                    .row(2L, null, null, null, null, null, null, null, null, null, null, null)
                    .row(3L, "bye", (byte) 46, (short) 346, 345, 456L, 754.2008f, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0L, false))
                    .build();

    protected static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", createUnboundedVarcharType()))
            .build();

    protected static final Predicate<String> PARTITION_COLUMN_FILTER = columnName -> columnName.equals("ds") || columnName.startsWith("part_");

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA = new MaterializedResult(
            CREATE_TABLE_DATA.getMaterializedRows().stream()
                    .map(row -> new MaterializedRow(row.getPrecision(), newArrayList(concat(row.getFields(), ImmutableList.of("2015-07-0" + row.getField(0))))))
                    .collect(toList()),
            ImmutableList.<Type>builder()
                    .addAll(CREATE_TABLE_DATA.getTypes())
                    .add(createUnboundedVarcharType())
                    .build());

    private static final String CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE = "2015-07-04";

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA_2ND =
            MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE, createUnboundedVarcharType())
                    .row(4L, "hello", (byte) 45, (short) 345, 234, 123L, 754.1985f, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1L, true), CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE)
                    .row(5L, null, null, null, null, null, null, null, null, null, null, null, CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE)
                    .row(6L, "bye", (byte) 46, (short) 346, 345, 456L, -754.2008f, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0L, false), CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE)
                    .build();

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("tinyint_to_smallint", TINYINT))
            .add(new ColumnMetadata("tinyint_to_integer", TINYINT))
            .add(new ColumnMetadata("tinyint_to_bigint", TINYINT))
            .add(new ColumnMetadata("smallint_to_integer", SMALLINT))
            .add(new ColumnMetadata("smallint_to_bigint", SMALLINT))
            .add(new ColumnMetadata("integer_to_bigint", INTEGER))
            .add(new ColumnMetadata("integer_to_varchar", INTEGER))
            //.add(new ColumnMetadata("varchar_to_integer", createUnboundedVarcharType())) // this coercion is not permitted in Hive 3. TODO test this on Hive < 3.
            .add(new ColumnMetadata("float_to_double", REAL))
            .add(new ColumnMetadata("varchar_to_drop_in_row", createUnboundedVarcharType()))
            .build();

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_TABLE_BEFORE = ImmutableList.<ColumnMetadata>builder()
            .addAll(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE)
            .add(new ColumnMetadata("struct_to_struct", toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE)))
            .add(new ColumnMetadata("list_to_list", arrayType(toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE))))
            .add(new ColumnMetadata("map_to_map", mapType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE.get(1).getType(), toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE))))
            .add(new ColumnMetadata("ds", createUnboundedVarcharType()))
            .build();

    private static RowType toRowType(List<ColumnMetadata> columns)
    {
        return rowType(columns.stream()
                .map(col -> new NamedTypeSignature(Optional.of(new RowFieldName(format("f_%s", col.getName()))), col.getType().getTypeSignature()))
                .collect(toImmutableList()));
    }

    private static final MaterializedResult MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_BEFORE =
            MaterializedResult.resultBuilder(SESSION, TINYINT, TINYINT, TINYINT, SMALLINT, SMALLINT, INTEGER, INTEGER, createUnboundedVarcharType(), REAL, createUnboundedVarcharType())
                    .row((byte) -11, (byte) 12, (byte) -13, (short) 14, (short) 15, -16, 17, /*"2147483647",*/ 18.0f, "2016-08-01")
                    .row((byte) 21, (byte) -22, (byte) 23, (short) -24, (short) 25, 26, -27, /*"asdf",*/ -28.0f, "2016-08-02")
                    .row((byte) -31, (byte) -32, (byte) 33, (short) 34, (short) -35, 36, 37, /*"-923",*/ 39.5f, "2016-08-03")
                    .row(null, (byte) 42, (byte) 43, (short) 44, (short) -45, 46, 47, /*"2147483648",*/ 49.5f, "2016-08-03")
                    .build();

    private static final MaterializedResult MISMATCH_SCHEMA_TABLE_DATA_BEFORE =
            MaterializedResult.resultBuilder(SESSION, MISMATCH_SCHEMA_TABLE_BEFORE.stream().map(ColumnMetadata::getType).collect(toImmutableList()))
                    .rows(MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_BEFORE.getMaterializedRows()
                            .stream()
                            .map(materializedRow -> {
                                List<Object> result = materializedRow.getFields();
                                List<Object> rowResult = materializedRow.getFields();
                                result.add(rowResult);
                                result.add(Arrays.asList(rowResult, null, rowResult));
                                result.add(ImmutableMap.of(rowResult.get(1), rowResult));
                                result.add(rowResult.get(8));
                                return new MaterializedRow(materializedRow.getPrecision(), result);
                            }).collect(toImmutableList()))
                    .build();

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("tinyint_to_smallint", SMALLINT))
            .add(new ColumnMetadata("tinyint_to_integer", INTEGER))
            .add(new ColumnMetadata("tinyint_to_bigint", BIGINT))
            .add(new ColumnMetadata("smallint_to_integer", INTEGER))
            .add(new ColumnMetadata("smallint_to_bigint", BIGINT))
            .add(new ColumnMetadata("integer_to_bigint", BIGINT))
            .add(new ColumnMetadata("integer_to_varchar", createUnboundedVarcharType()))
            //.add(new ColumnMetadata("varchar_to_integer", INTEGER))
            .add(new ColumnMetadata("float_to_double", DOUBLE))
            .add(new ColumnMetadata("varchar_to_drop_in_row", createUnboundedVarcharType()))
            .build();

    private static final Type MISMATCH_SCHEMA_ROW_TYPE_APPEND = toRowType(ImmutableList.<ColumnMetadata>builder()
            .addAll(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER)
            .add(new ColumnMetadata(format("%s_append", MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.get(0).getName()), MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.get(0).getType()))
            .build());
    private static final Type MISMATCH_SCHEMA_ROW_TYPE_DROP = toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.subList(0, MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.size() - 1));

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_TABLE_AFTER = ImmutableList.<ColumnMetadata>builder()
            .addAll(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER)
            .add(new ColumnMetadata("struct_to_struct", MISMATCH_SCHEMA_ROW_TYPE_APPEND))
            .add(new ColumnMetadata("list_to_list", arrayType(MISMATCH_SCHEMA_ROW_TYPE_APPEND)))
            .add(new ColumnMetadata("map_to_map", mapType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.get(1).getType(), MISMATCH_SCHEMA_ROW_TYPE_DROP)))
            .add(new ColumnMetadata("ds", createUnboundedVarcharType()))
            .build();

    private static final MaterializedResult MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_AFTER =
            MaterializedResult.resultBuilder(SESSION, SMALLINT, INTEGER, BIGINT, INTEGER, BIGINT, BIGINT, createUnboundedVarcharType(), INTEGER, DOUBLE, createUnboundedVarcharType())
                    .row((short) -11, 12, -13L, 14, 15L, -16L, "17", /*2147483647,*/ 18.0, "2016-08-01")
                    .row((short) 21, -22, 23L, -24, 25L, 26L, "-27", /*null,*/ -28.0, "2016-08-02")
                    .row((short) -31, -32, 33L, 34, -35L, 36L, "37", /*-923,*/ 39.5, "2016-08-03")
                    .row(null, 42, 43L, 44, -45L, 46L, "47", /*null,*/ 49.5, "2016-08-03")
                    .build();

    private static final MaterializedResult MISMATCH_SCHEMA_TABLE_DATA_AFTER =
            MaterializedResult.resultBuilder(SESSION, MISMATCH_SCHEMA_TABLE_AFTER.stream().map(ColumnMetadata::getType).collect(toImmutableList()))
                    .rows(MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_AFTER.getMaterializedRows()
                            .stream()
                            .map(materializedRow -> {
                                List<Object> result = materializedRow.getFields();
                                List<Object> appendFieldRowResult = materializedRow.getFields();
                                appendFieldRowResult.add(null);
                                List<Object> dropFieldRowResult = materializedRow.getFields().subList(0, materializedRow.getFields().size() - 1);
                                result.add(appendFieldRowResult);
                                result.add(Arrays.asList(appendFieldRowResult, null, appendFieldRowResult));
                                result.add(ImmutableMap.of(result.get(1), dropFieldRowResult));
                                result.add(result.get(8));
                                return new MaterializedRow(materializedRow.getPrecision(), result);
                            }).collect(toImmutableList()))
                    .build();

    protected Set<HiveStorageFormat> createTableFormats = difference(
            ImmutableSet.copyOf(HiveStorageFormat.values()),
            // exclude formats that change table schema with serde
            ImmutableSet.of(AVRO, CSV));

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(TYPE_OPERATORS);
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(TYPE_OPERATORS);

    protected static final List<ColumnMetadata> STATISTICS_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("t_boolean", BOOLEAN))
            .add(new ColumnMetadata("t_bigint", BIGINT))
            .add(new ColumnMetadata("t_integer", INTEGER))
            .add(new ColumnMetadata("t_smallint", SMALLINT))
            .add(new ColumnMetadata("t_tinyint", TINYINT))
            .add(new ColumnMetadata("t_double", DOUBLE))
            .add(new ColumnMetadata("t_float", REAL))
            .add(new ColumnMetadata("t_string", createUnboundedVarcharType()))
            .add(new ColumnMetadata("t_varchar", createVarcharType(100)))
            .add(new ColumnMetadata("t_char", createCharType(5)))
            .add(new ColumnMetadata("t_varbinary", VARBINARY))
            .add(new ColumnMetadata("t_date", DATE))
            .add(new ColumnMetadata("t_timestamp", TIMESTAMP_MILLIS))
            .add(new ColumnMetadata("t_short_decimal", createDecimalType(5, 2)))
            .add(new ColumnMetadata("t_long_decimal", createDecimalType(20, 3)))
            .build();

    protected static final List<ColumnMetadata> STATISTICS_PARTITIONED_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .addAll(STATISTICS_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", VARCHAR))
            .build();

    protected static final PartitionStatistics EMPTY_TABLE_STATISTICS = new PartitionStatistics(createZeroStatistics(), ImmutableMap.of());
    protected static final PartitionStatistics BASIC_STATISTICS_1 = new PartitionStatistics(new HiveBasicStatistics(0, 20, 3, 0), ImmutableMap.of());
    protected static final PartitionStatistics BASIC_STATISTICS_2 = new PartitionStatistics(new HiveBasicStatistics(0, 30, 2, 0), ImmutableMap.of());

    protected static final PartitionStatistics STATISTICS_1 =
            new PartitionStatistics(
                    BASIC_STATISTICS_1.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_boolean", createBooleanColumnStatistics(OptionalLong.of(5), OptionalLong.of(6), OptionalLong.of(3)))
                            .put("t_bigint", createIntegerColumnStatistics(OptionalLong.of(1234L), OptionalLong.of(5678L), OptionalLong.of(2), OptionalLong.of(5)))
                            .put("t_integer", createIntegerColumnStatistics(OptionalLong.of(123L), OptionalLong.of(567L), OptionalLong.of(3), OptionalLong.of(4)))
                            .put("t_smallint", createIntegerColumnStatistics(OptionalLong.of(12L), OptionalLong.of(56L), OptionalLong.of(2), OptionalLong.of(6)))
                            .put("t_tinyint", createIntegerColumnStatistics(OptionalLong.of(1L), OptionalLong.of(2L), OptionalLong.of(1), OptionalLong.of(3)))
                            .put("t_double", createDoubleColumnStatistics(OptionalDouble.of(1234.25), OptionalDouble.of(5678.58), OptionalLong.of(7), OptionalLong.of(8)))
                            .put("t_float", createDoubleColumnStatistics(OptionalDouble.of(123.25), OptionalDouble.of(567.58), OptionalLong.of(9), OptionalLong.of(10)))
                            .put("t_string", createStringColumnStatistics(OptionalLong.of(10), OptionalLong.of(50), OptionalLong.of(3), OptionalLong.of(7)))
                            .put("t_varchar", createStringColumnStatistics(OptionalLong.of(100), OptionalLong.of(230), OptionalLong.of(5), OptionalLong.of(3)))
                            .put("t_char", createStringColumnStatistics(OptionalLong.of(5), OptionalLong.of(50), OptionalLong.of(1), OptionalLong.of(4)))
                            .put("t_varbinary", createBinaryColumnStatistics(OptionalLong.of(4), OptionalLong.of(50), OptionalLong.of(1)))
                            .put("t_date", createDateColumnStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)), OptionalLong.of(7), OptionalLong.of(6)))
                            .put("t_timestamp", createIntegerColumnStatistics(OptionalLong.of(1234567L), OptionalLong.of(71234567L), OptionalLong.of(7), OptionalLong.of(5)))
                            .put("t_short_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal(10)), Optional.of(new BigDecimal(12)), OptionalLong.of(3), OptionalLong.of(5)))
                            .put("t_long_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal("12345678901234567.123")), Optional.of(new BigDecimal("81234567890123456.123")), OptionalLong.of(2), OptionalLong.of(1)))
                            .buildOrThrow());

    protected static final PartitionStatistics STATISTICS_1_1 =
            new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(15), OptionalLong.empty(), OptionalLong.of(0)),
                    STATISTICS_1.getColumnStatistics().entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().hashCode() % 2 == 0)
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

    protected static final PartitionStatistics STATISTICS_1_2 =
            new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(15), OptionalLong.of(3), OptionalLong.of(0)),
                    STATISTICS_1.getColumnStatistics().entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().hashCode() % 2 == 1)
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

    private static final PartitionStatistics STATISTICS_2 =
            new PartitionStatistics(
                    BASIC_STATISTICS_2.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_boolean", createBooleanColumnStatistics(OptionalLong.of(4), OptionalLong.of(3), OptionalLong.of(2)))
                            .put("t_bigint", createIntegerColumnStatistics(OptionalLong.of(2345L), OptionalLong.of(6789L), OptionalLong.of(4), OptionalLong.of(7)))
                            .put("t_integer", createIntegerColumnStatistics(OptionalLong.of(234L), OptionalLong.of(678L), OptionalLong.of(5), OptionalLong.of(6)))
                            .put("t_smallint", createIntegerColumnStatistics(OptionalLong.of(23L), OptionalLong.of(65L), OptionalLong.of(7), OptionalLong.of(5)))
                            .put("t_tinyint", createIntegerColumnStatistics(OptionalLong.of(3L), OptionalLong.of(12L), OptionalLong.of(2), OptionalLong.of(3)))
                            .put("t_double", createDoubleColumnStatistics(OptionalDouble.of(2345.25), OptionalDouble.of(6785.58), OptionalLong.of(6), OptionalLong.of(3)))
                            .put("t_float", createDoubleColumnStatistics(OptionalDouble.of(235.25), OptionalDouble.of(676.58), OptionalLong.of(7), OptionalLong.of(11)))
                            .put("t_string", createStringColumnStatistics(OptionalLong.of(301), OptionalLong.of(600), OptionalLong.of(2), OptionalLong.of(6)))
                            .put("t_varchar", createStringColumnStatistics(OptionalLong.of(99), OptionalLong.of(223), OptionalLong.of(7), OptionalLong.of(1)))
                            .put("t_char", createStringColumnStatistics(OptionalLong.of(6), OptionalLong.of(60), OptionalLong.of(0), OptionalLong.of(3)))
                            .put("t_varbinary", createBinaryColumnStatistics(OptionalLong.of(2), OptionalLong.of(10), OptionalLong.of(2)))
                            .put("t_date", createDateColumnStatistics(Optional.of(LocalDate.ofEpochDay(2)), Optional.of(LocalDate.ofEpochDay(3)), OptionalLong.of(8), OptionalLong.of(7)))
                            .put("t_timestamp", createIntegerColumnStatistics(OptionalLong.of(2345671L), OptionalLong.of(12345677L), OptionalLong.of(9), OptionalLong.of(1)))
                            .put("t_short_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal(11)), Optional.of(new BigDecimal(14)), OptionalLong.of(5), OptionalLong.of(7)))
                            .put("t_long_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal("71234567890123456.123")), Optional.of(new BigDecimal("78123456789012345.123")), OptionalLong.of(2), OptionalLong.of(1)))
                            .buildOrThrow());

    private static final PartitionStatistics STATISTICS_EMPTY_OPTIONAL_FIELDS =
            new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(20), OptionalLong.empty(), OptionalLong.of(0)),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_boolean", createBooleanColumnStatistics(OptionalLong.of(4), OptionalLong.of(3), OptionalLong.of(2)))
                            .put("t_bigint", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(4), OptionalLong.of(7)))
                            .put("t_integer", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(5), OptionalLong.of(6)))
                            .put("t_smallint", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(7), OptionalLong.of(5)))
                            .put("t_tinyint", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(2), OptionalLong.of(3)))
                            .put("t_double", createDoubleColumnStatistics(OptionalDouble.empty(), OptionalDouble.empty(), OptionalLong.of(6), OptionalLong.of(3)))
                            .put("t_float", createDoubleColumnStatistics(OptionalDouble.empty(), OptionalDouble.empty(), OptionalLong.of(7), OptionalLong.of(11)))
                            .put("t_string", createStringColumnStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(2), OptionalLong.of(6)))
                            .put("t_varchar", createStringColumnStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(7), OptionalLong.of(1)))
                            .put("t_char", createStringColumnStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(3)))
                            .put("t_varbinary", createBinaryColumnStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(2)))
                            // https://issues.apache.org/jira/browse/HIVE-20098
                            // .put("t_date", createDateColumnStatistics(Optional.empty(), Optional.empty(), OptionalLong.of(8), OptionalLong.of(7)))
                            .put("t_timestamp", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(9), OptionalLong.of(1)))
                            .put("t_short_decimal", createDecimalColumnStatistics(Optional.empty(), Optional.empty(), OptionalLong.of(5), OptionalLong.of(7)))
                            .put("t_long_decimal", createDecimalColumnStatistics(Optional.empty(), Optional.empty(), OptionalLong.of(2), OptionalLong.of(1)))
                            .buildOrThrow());

    protected String database;
    protected SchemaTableName tablePartitionFormat;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName tableOffline;
    protected SchemaTableName tableOfflinePartition;
    protected SchemaTableName tableNotReadable;
    protected SchemaTableName view;
    protected SchemaTableName invalidTable;
    protected SchemaTableName tableBucketedStringInt;
    protected SchemaTableName tableBucketedBigintBoolean;
    protected SchemaTableName tableBucketedDoubleFloat;
    protected SchemaTableName tablePartitionSchemaChange;
    protected SchemaTableName tablePartitionSchemaChangeNonCanonical;
    protected SchemaTableName tableBucketEvolution;

    protected ConnectorTableHandle invalidTableHandle;

    protected ColumnHandle dsColumn;
    protected ColumnHandle fileFormatColumn;
    protected ColumnHandle dummyColumn;
    protected ColumnHandle intColumn;
    protected ColumnHandle invalidColumnHandle;

    protected ConnectorTableProperties tablePartitionFormatProperties;
    protected ConnectorTableProperties tableUnpartitionedProperties;
    protected List<HivePartition> tablePartitionFormatPartitions;
    protected List<HivePartition> tableUnpartitionedPartitions;

    protected HdfsEnvironment hdfsEnvironment;
    protected LocationService locationService;

    protected HiveMetadataFactory metadataFactory;
    protected HiveTransactionManager transactionManager;
    protected HiveMetastore metastoreClient;
    protected ConnectorSplitManager splitManager;
    protected ConnectorPageSourceProvider pageSourceProvider;
    protected ConnectorPageSinkProvider pageSinkProvider;
    protected ConnectorNodePartitioningProvider nodePartitioningProvider;
    protected ExecutorService executor;

    private ScheduledExecutorService heartbeatService;
    private java.nio.file.Path temporaryStagingDirectory;

    @BeforeClass(alwaysRun = true)
    public void setupClass()
            throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
        heartbeatService = newScheduledThreadPool(1);
        // Use separate staging directory for each test class to prevent intermittent failures coming from test parallelism
        temporaryStagingDirectory = createTempDirectory("trino-staging-");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        if (heartbeatService != null) {
            heartbeatService.shutdownNow();
            heartbeatService = null;
        }
        if (temporaryStagingDirectory != null) {
            try {
                deleteRecursively(temporaryStagingDirectory, ALLOW_INSECURE);
            }
            catch (Exception e) {
                log.warn(e, "Error deleting %s", temporaryStagingDirectory);
            }
        }
    }

    protected void setupHive(String databaseName)
    {
        database = databaseName;
        tablePartitionFormat = new SchemaTableName(database, "trino_test_partition_format");
        tableUnpartitioned = new SchemaTableName(database, "trino_test_unpartitioned");
        tableOffline = new SchemaTableName(database, "trino_test_offline");
        tableOfflinePartition = new SchemaTableName(database, "trino_test_offline_partition");
        tableNotReadable = new SchemaTableName(database, "trino_test_not_readable");
        view = new SchemaTableName(database, "trino_test_view");
        invalidTable = new SchemaTableName(database, INVALID_TABLE);
        tableBucketedStringInt = new SchemaTableName(database, "trino_test_bucketed_by_string_int");
        tableBucketedBigintBoolean = new SchemaTableName(database, "trino_test_bucketed_by_bigint_boolean");
        tableBucketedDoubleFloat = new SchemaTableName(database, "trino_test_bucketed_by_double_float");
        tablePartitionSchemaChange = new SchemaTableName(database, "trino_test_partition_schema_change");
        tablePartitionSchemaChangeNonCanonical = new SchemaTableName(database, "trino_test_partition_schema_change_non_canonical");
        tableBucketEvolution = new SchemaTableName(database, "trino_test_bucket_evolution");

        invalidTableHandle = new HiveTableHandle(database, INVALID_TABLE, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());

        dsColumn = createBaseColumn("ds", -1, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
        fileFormatColumn = createBaseColumn("file_format", -1, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
        dummyColumn = createBaseColumn("dummy", -1, HIVE_INT, INTEGER, PARTITION_KEY, Optional.empty());
        intColumn = createBaseColumn("t_int", -1, HIVE_INT, INTEGER, PARTITION_KEY, Optional.empty());
        invalidColumnHandle = createBaseColumn(INVALID_COLUMN, 0, HIVE_STRING, VARCHAR, REGULAR, Optional.empty());

        List<ColumnHandle> partitionColumns = ImmutableList.of(dsColumn, fileFormatColumn, dummyColumn);
        tablePartitionFormatPartitions = ImmutableList.<HivePartition>builder()
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=textfile/dummy=1",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("textfile")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 1L))
                                .buildOrThrow()))
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=sequencefile/dummy=2",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("sequencefile")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 2L))
                                .buildOrThrow()))
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=rctext/dummy=3",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("rctext")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 3L))
                                .buildOrThrow()))
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=rcbinary/dummy=4",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("rcbinary")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 4L))
                                .buildOrThrow()))
                .build();
        tableUnpartitionedPartitions = ImmutableList.of(new HivePartition(tableUnpartitioned));
        tablePartitionFormatProperties = new ConnectorTableProperties(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                        fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("textfile")), Range.equal(createUnboundedVarcharType(), utf8Slice("sequencefile")), Range.equal(createUnboundedVarcharType(), utf8Slice("rctext")), Range.equal(createUnboundedVarcharType(), utf8Slice("rcbinary"))), false),
                        dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 1L), Range.equal(INTEGER, 2L), Range.equal(INTEGER, 3L), Range.equal(INTEGER, 4L)), false))),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DiscretePredicates(partitionColumns, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("textfile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 1L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("sequencefile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 2L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("rctext"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 3L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("rcbinary"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 4L)), false)))))),
                ImmutableList.of());
        tableUnpartitionedProperties = new ConnectorTableProperties();
    }

    protected final void setup(String host, int port, String databaseName, String timeZone)
    {
        HiveConfig hiveConfig = getHiveConfig()
                .setParquetTimeZone(timeZone)
                .setRcfileTimeZone(timeZone);

        Optional<HostAndPort> proxy = Optional.ofNullable(System.getProperty("hive.metastore.thrift.client.socks-proxy"))
                .map(HostAndPort::fromString);

        MetastoreLocator metastoreLocator = new TestingMetastoreLocator(proxy, HostAndPort.fromParts(host, port));

        hdfsEnvironment = new HdfsEnvironment(createTestHdfsConfiguration(), new HdfsConfig(), new NoHdfsAuthentication());
        HiveMetastore metastore = cachingHiveMetastore(
                new BridgingHiveMetastore(new ThriftHiveMetastore(
                        metastoreLocator,
                        hiveConfig,
                        new MetastoreConfig(),
                        new ThriftMetastoreConfig(),
                        hdfsEnvironment,
                        false),
                        new HiveIdentity(SESSION.getIdentity())),
                executor,
                new Duration(1, MINUTES),
                Optional.of(new Duration(15, SECONDS)),
                10000);

        setup(databaseName, hiveConfig, metastore, hdfsEnvironment);
    }

    protected final void setup(String databaseName, HiveConfig hiveConfig, HiveMetastore hiveMetastore, HdfsEnvironment hdfsConfiguration)
    {
        setupHive(databaseName);

        metastoreClient = hiveMetastore;
        hdfsEnvironment = hdfsConfiguration;
        HivePartitionManager partitionManager = new HivePartitionManager(hiveConfig);
        locationService = new HiveLocationService(hdfsEnvironment);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        metadataFactory = new HiveMetadataFactory(
                new CatalogName("hive"),
                HiveMetastoreFactory.ofInstance(metastoreClient),
                hdfsEnvironment,
                partitionManager,
                10,
                10,
                10,
                false,
                false,
                false,
                true,
                true,
                false,
                1000,
                Optional.empty(),
                true,
                TESTING_TYPE_MANAGER,
                NOOP_METADATA_PROVIDER,
                locationService,
                partitionUpdateCodec,
                newFixedThreadPool(2),
                heartbeatService,
                TEST_SERVER_VERSION,
                (session, tableHandle) -> {
                    if (!tableHandle.getTableName().contains("apply_redirection_tester")) {
                        return Optional.empty();
                    }
                    return Optional.of(new TableScanRedirectApplicationResult(
                            new CatalogSchemaTableName("hive", databaseName, "mock_redirection_target"),
                            ImmutableMap.of(),
                            TupleDomain.all()));
                },
                ImmutableSet.of(
                        new PartitionsSystemTableProvider(partitionManager, TESTING_TYPE_MANAGER),
                        new PropertiesSystemTableProvider()),
                metastore -> new NoneHiveMaterializedViewMetadata()
                {
                    @Override
                    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
                    {
                        if (!viewName.getTableName().contains("materialized_view_tester")) {
                            return Optional.empty();
                        }
                        return Optional.of(new ConnectorMaterializedViewDefinition(
                                "dummy_view_sql",
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("abc", TypeId.of("type"))),
                                Optional.empty(),
                                Optional.of("alice"),
                                ImmutableMap.of()));
                    }
                },
                SqlStandardAccessControlMetadata::new,
                NO_REDIRECTIONS,
                TableInvalidationCallback.NOOP);
        transactionManager = new HiveTransactionManager(metadataFactory);
        splitManager = new HiveSplitManager(
                transactionManager,
                partitionManager,
                new NamenodeStats(),
                hdfsEnvironment,
                new CachingDirectoryLister(hiveConfig),
                directExecutor(),
                new CounterStat(),
                100,
                hiveConfig.getMaxOutstandingSplitsSize(),
                hiveConfig.getMinPartitionBatchSize(),
                hiveConfig.getMaxPartitionBatchSize(),
                hiveConfig.getMaxInitialSplits(),
                hiveConfig.getSplitLoaderConcurrency(),
                hiveConfig.getMaxSplitsPerSecond(),
                false,
                TESTING_TYPE_MANAGER);
        pageSinkProvider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(hiveConfig, hdfsEnvironment),
                hdfsEnvironment,
                PAGE_SORTER,
                HiveMetastoreFactory.ofInstance(metastoreClient),
                new GroupByHashPageIndexerFactory(JOIN_COMPILER, BLOCK_TYPE_OPERATORS),
                TESTING_TYPE_MANAGER,
                getHiveConfig(),
                locationService,
                partitionUpdateCodec,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                getHiveSessionProperties(hiveConfig),
                new HiveWriterStats());
        pageSourceProvider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                hdfsEnvironment,
                hiveConfig,
                getDefaultHivePageSourceFactories(hdfsEnvironment, hiveConfig),
                getDefaultHiveRecordCursorProviders(hiveConfig, hdfsEnvironment),
                new GenericHiveRecordCursorProvider(hdfsEnvironment, hiveConfig),
                Optional.empty());
        nodePartitioningProvider = new HiveNodePartitioningProvider(
                new TestingNodeManager("fake-environment"),
                TESTING_TYPE_MANAGER);
    }

    protected HdfsConfiguration createTestHdfsConfiguration()
    {
        return new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig()
                                .setSocksProxy(Optional.ofNullable(System.getProperty("hive.hdfs.socks-proxy"))
                                        .map(HostAndPort::fromString)
                                        .orElse(null)),
                        ImmutableSet.of(
                                new TrinoS3ConfigurationInitializer(new HiveS3Config()),
                                new GoogleGcsConfigurationInitializer(new HiveGcsConfig()),
                                new TrinoAzureConfigurationInitializer(new HiveAzureConfig()))),
                ImmutableSet.of());
    }

    /**
     * Allow subclass to change default configuration.
     */
    protected HiveConfig getHiveConfig()
    {
        return new HiveConfig()
                .setMaxOpenSortFiles(10)
                .setTemporaryStagingDirectoryPath(temporaryStagingDirectory.toAbsolutePath().toString())
                .setWriterSortBufferSize(DataSize.of(100, KILOBYTE));
    }

    protected ConnectorSession newSession()
    {
        return newSession(ImmutableMap.of());
    }

    protected ConnectorSession newSession(Map<String, Object> propertyValues)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(getHiveConfig()).getSessionProperties())
                .setPropertyValues(propertyValues)
                .build();
    }

    protected Transaction newTransaction()
    {
        return new HiveTransaction(transactionManager);
    }

    protected interface Transaction
            extends AutoCloseable
    {
        ConnectorMetadata getMetadata();

        SemiTransactionalHiveMetastore getMetastore();

        ConnectorTransactionHandle getTransactionHandle();

        void commit();

        void rollback();

        @Override
        void close();
    }

    static class HiveTransaction
            implements Transaction
    {
        private final HiveTransactionManager transactionManager;
        private final ConnectorTransactionHandle transactionHandle;
        private boolean closed;

        public HiveTransaction(HiveTransactionManager transactionManager)
        {
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.transactionHandle = new HiveTransactionHandle(false);
            transactionManager.begin(transactionHandle);
            getMetastore().testOnlyThrowOnCleanupFailures();
        }

        @Override
        public ConnectorMetadata getMetadata()
        {
            return transactionManager.get(transactionHandle, SESSION.getIdentity());
        }

        @Override
        public SemiTransactionalHiveMetastore getMetastore()
        {
            return transactionManager.get(transactionHandle, SESSION.getIdentity()).getMetastore();
        }

        @Override
        public ConnectorTransactionHandle getTransactionHandle()
        {
            return transactionHandle;
        }

        @Override
        public void commit()
        {
            checkState(!closed);
            closed = true;
            transactionManager.commit(transactionHandle);
        }

        @Override
        public void rollback()
        {
            checkState(!closed);
            closed = true;
            transactionManager.rollback(transactionHandle);
        }

        @Override
        public void close()
        {
            if (!closed) {
                try {
                    getMetastore().testOnlyCheckIsReadOnly(); // transactions in this test with writes in it must explicitly commit or rollback
                }
                finally {
                    rollback();
                }
            }
        }
    }

    @Test
    public void testGetDatabaseNames()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<String> databases = metadata.listSchemaNames(newSession());
            assertTrue(databases.contains(database));
        }
    }

    @Test
    public void testGetTableNames()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<SchemaTableName> tables = metadata.listTables(newSession(), Optional.of(database));
            assertTrue(tables.contains(tablePartitionFormat));
            assertTrue(tables.contains(tableUnpartitioned));
        }
    }

    @Test
    public void testGetAllTableNames()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<SchemaTableName> tables = metadata.listTables(newSession(), Optional.empty());
            assertTrue(tables.contains(tablePartitionFormat));
            assertTrue(tables.contains(tableUnpartitioned));
        }
    }

    @Test
    public void testGetAllTableColumns()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> allColumns = listTableColumns(metadata, newSession(), new SchemaTablePrefix());
            assertTrue(allColumns.containsKey(tablePartitionFormat));
            assertTrue(allColumns.containsKey(tableUnpartitioned));
        }
    }

    @Test
    public void testGetAllTableColumnsInSchema()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> allColumns = listTableColumns(metadata, newSession(), new SchemaTablePrefix(database));
            assertTrue(allColumns.containsKey(tablePartitionFormat));
            assertTrue(allColumns.containsKey(tableUnpartitioned));
        }
    }

    @Test
    public void testListUnknownSchema()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            assertNull(metadata.getTableHandle(session, new SchemaTableName(INVALID_DATABASE, INVALID_TABLE)));
            assertEquals(metadata.listTables(session, Optional.of(INVALID_DATABASE)), ImmutableList.of());
            assertEquals(listTableColumns(metadata, session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
            assertEquals(metadata.listViews(session, Optional.of(INVALID_DATABASE)), ImmutableList.of());
            assertEquals(metadata.getViews(session, Optional.of(INVALID_DATABASE)), ImmutableMap.of());
            assertEquals(metadata.getView(session, new SchemaTableName(INVALID_DATABASE, INVALID_TABLE)), Optional.empty());
        }
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            tableHandle = applyFilter(metadata, tableHandle, Constraint.alwaysTrue());
            ConnectorTableProperties properties = metadata.getTableProperties(newSession(), tableHandle);
            assertExpectedTableProperties(properties, tablePartitionFormatProperties);
            assertExpectedPartitions(tableHandle, tablePartitionFormatPartitions);
        }
    }

    @Test
    public void testGetPartitionsWithBindings()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            Constraint constraint = new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(intColumn, Domain.singleValue(BIGINT, 5L))));
            tableHandle = applyFilter(metadata, tableHandle, constraint);
            ConnectorTableProperties properties = metadata.getTableProperties(newSession(), tableHandle);
            assertExpectedTableProperties(properties, tablePartitionFormatProperties);
            assertExpectedPartitions(tableHandle, tablePartitionFormatPartitions);
        }
    }

    @Test
    public void testMismatchSchemaTable()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            // TODO: fix coercion for JSON
            if (storageFormat == JSON) {
                continue;
            }
            SchemaTableName temporaryMismatchSchemaTable = temporaryTable("mismatch_schema");
            try {
                doTestMismatchSchemaTable(
                        temporaryMismatchSchemaTable,
                        storageFormat,
                        MISMATCH_SCHEMA_TABLE_BEFORE,
                        MISMATCH_SCHEMA_TABLE_DATA_BEFORE,
                        MISMATCH_SCHEMA_TABLE_AFTER,
                        MISMATCH_SCHEMA_TABLE_DATA_AFTER);
            }
            finally {
                dropTable(temporaryMismatchSchemaTable);
            }
        }
    }

    protected void doTestMismatchSchemaTable(
            SchemaTableName schemaTableName,
            HiveStorageFormat storageFormat,
            List<ColumnMetadata> tableBefore,
            MaterializedResult dataBefore,
            List<ColumnMetadata> tableAfter,
            MaterializedResult dataAfter)
            throws Exception
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        doCreateEmptyTable(schemaTableName, storageFormat, tableBefore);

        // insert the data
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);

            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(dataBefore.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());

            transaction.commit();
        }

        // load the table and verify the data
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);

            List<ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toList());

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), dataBefore.getMaterializedRows());
            transaction.commit();
        }

        // alter the table schema
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(session);
            Table oldTable = transaction.getMetastore().getTable(schemaName, tableName).get();
            List<Column> dataColumns = tableAfter.stream()
                    .filter(columnMetadata -> !columnMetadata.getName().equals("ds"))
                    .map(columnMetadata -> new Column(columnMetadata.getName(), toHiveType(columnMetadata.getType()), Optional.empty()))
                    .collect(toList());
            Table.Builder newTable = Table.builder(oldTable)
                    .setDataColumns(dataColumns);

            transaction.getMetastore().replaceTable(schemaName, tableName, newTable.build(), principalPrivileges);

            transaction.commit();
        }

        // load the altered table and verify the data
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);
            List<ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toList());

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), dataAfter.getMaterializedRows());

            transaction.commit();
        }

        // insertions to the partitions with type mismatches should fail
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);

            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(dataAfter.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());

            transaction.commit();

            fail("expected exception");
        }
        catch (TrinoException e) {
            // expected
            assertEquals(e.getErrorCode(), HIVE_PARTITION_SCHEMA_MISMATCH.toErrorCode());
        }
    }

    protected void assertExpectedTableProperties(ConnectorTableProperties actualProperties, ConnectorTableProperties expectedProperties)
    {
        assertEquals(actualProperties.getPredicate(), expectedProperties.getPredicate());
        assertEquals(actualProperties.getDiscretePredicates().isPresent(), expectedProperties.getDiscretePredicates().isPresent());
        actualProperties.getDiscretePredicates().ifPresent(actual -> {
            DiscretePredicates expected = expectedProperties.getDiscretePredicates().get();
            assertEquals(actual.getColumns(), expected.getColumns());
            assertEqualsIgnoreOrder(actual.getPredicates(), expected.getPredicates());
        });
        assertEquals(actualProperties.getStreamPartitioningColumns(), expectedProperties.getStreamPartitioningColumns());
        assertEquals(actualProperties.getLocalProperties(), expectedProperties.getLocalProperties());
    }

    protected void assertExpectedPartitions(ConnectorTableHandle table, Iterable<HivePartition> expectedPartitions)
    {
        Iterable<HivePartition> actualPartitions = ((HiveTableHandle) table).getPartitions().orElseThrow(AssertionError::new);
        Map<String, ?> actualById = uniqueIndex(actualPartitions, HivePartition::getPartitionId);
        for (Object expected : expectedPartitions) {
            assertInstanceOf(expected, HivePartition.class);
            HivePartition expectedPartition = (HivePartition) expected;

            Object actual = actualById.get(expectedPartition.getPartitionId());
            assertEquals(actual, expected);
            assertInstanceOf(actual, HivePartition.class);
            HivePartition actualPartition = (HivePartition) actual;

            assertNotNull(actualPartition, "partition " + expectedPartition.getPartitionId());
            assertEquals(actualPartition.getPartitionId(), expectedPartition.getPartitionId());
            assertEquals(actualPartition.getKeys(), expectedPartition.getKeys());
            assertEquals(actualPartition.getTableName(), expectedPartition.getTableName());
        }
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            tableHandle = applyFilter(metadata, tableHandle, Constraint.alwaysTrue());
            ConnectorTableProperties properties = metadata.getTableProperties(newSession(), tableHandle);
            assertExpectedTableProperties(properties, new ConnectorTableProperties());
            assertExpectedPartitions(tableHandle, tableUnpartitionedPartitions);
        }
    }

    @Test
    public void testGetTableSchemaPartitionFormat()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), getTableHandle(metadata, tablePartitionFormat));
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
            assertPrimitiveField(map, "t_tinyint", TINYINT, false);
            assertPrimitiveField(map, "t_smallint", SMALLINT, false);
            assertPrimitiveField(map, "t_int", INTEGER, false);
            assertPrimitiveField(map, "t_bigint", BIGINT, false);
            assertPrimitiveField(map, "t_float", REAL, false);
            assertPrimitiveField(map, "t_double", DOUBLE, false);
            assertPrimitiveField(map, "t_boolean", BOOLEAN, false);
            assertPrimitiveField(map, "ds", createUnboundedVarcharType(), true);
            assertPrimitiveField(map, "file_format", createUnboundedVarcharType(), true);
            assertPrimitiveField(map, "dummy", INTEGER, true);
        }
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
            assertPrimitiveField(map, "t_tinyint", TINYINT, false);
        }
    }

    @Test
    public void testGetTableSchemaOffline()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> columns = listTableColumns(metadata, newSession(), tableOffline.toSchemaTablePrefix());
            assertEquals(columns.size(), 1);
            Map<String, ColumnMetadata> map = uniqueIndex(getOnlyElement(columns.values()), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOfflinePartition);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaNotReadablePartition()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableNotReadable);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaException()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertNull(metadata.getTableHandle(newSession(), invalidTable));
        }
    }

    @Test
    public void testGetTableStatsBucketedStringInt()
    {
        assertTableStatsComputed(
                tableBucketedStringInt,
                ImmutableSet.of(
                        "t_bigint",
                        "t_boolean",
                        "t_double",
                        "t_float",
                        "t_int",
                        "t_smallint",
                        "t_string",
                        "t_tinyint",
                        "ds"));
    }

    @Test
    public void testGetTableStatsUnpartitioned()
    {
        assertTableStatsComputed(
                tableUnpartitioned,
                ImmutableSet.of("t_string", "t_tinyint"));
    }

    private void assertTableStatsComputed(
            SchemaTableName tableName,
            Set<String> expectedColumnStatsColumns)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, Constraint.alwaysTrue());

            assertFalse(tableStatistics.getRowCount().isUnknown(), "row count is unknown");

            Map<String, ColumnStatistics> columnsStatistics = tableStatistics
                    .getColumnStatistics()
                    .entrySet()
                    .stream()
                    .collect(
                            toImmutableMap(
                                    entry -> ((HiveColumnHandle) entry.getKey()).getName(),
                                    Map.Entry::getValue));

            assertEquals(columnsStatistics.keySet(), expectedColumnStatsColumns, "columns with statistics");

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            columnsStatistics.forEach((columnName, columnStatistics) -> {
                ColumnHandle columnHandle = columnHandles.get(columnName);
                Type columnType = metadata.getColumnMetadata(session, tableHandle, columnHandle).getType();

                assertFalse(
                        columnStatistics.getNullsFraction().isUnknown(),
                        "unknown nulls fraction for " + columnName);

                assertFalse(
                        columnStatistics.getDistinctValuesCount().isUnknown(),
                        "unknown distinct values count for " + columnName);

                if (columnType instanceof VarcharType) {
                    assertFalse(
                            columnStatistics.getDataSize().isUnknown(),
                            "unknown data size for " + columnName);
                }
                else {
                    assertTrue(
                            columnStatistics.getDataSize().isUnknown(),
                            "unknown data size for" + columnName);
                }
            });
        }
    }

    @Test
    public void testGetPartitionSplitsBatch()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            ConnectorSplitSource splitSource = getSplits(splitManager, transaction, session, tableHandle);

            assertEquals(getSplitCount(splitSource), tablePartitionFormatPartitions.size());
        }
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            ConnectorSplitSource splitSource = getSplits(splitManager, transaction, session, tableHandle);

            assertEquals(getSplitCount(splitSource), 1);
        }
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
    {
        try (Transaction transaction = newTransaction()) {
            getSplits(splitManager, transaction, newSession(), invalidTableHandle);
        }
    }

    @Test
    public void testGetPartitionTableOffline()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            try {
                getTableHandle(metadata, tableOffline);
                fail("expected TableOfflineException");
            }
            catch (TableOfflineException e) {
                assertEquals(e.getTableName(), tableOffline);
            }
        }
    }

    @Test
    public void testGetPartitionSplitsTableOfflinePartition()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOfflinePartition);
            assertNotNull(tableHandle);

            ColumnHandle dsColumn = metadata.getColumnHandles(session, tableHandle).get("ds");
            assertNotNull(dsColumn);

            Domain domain = Domain.singleValue(createUnboundedVarcharType(), utf8Slice("2012-12-30"));
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(dsColumn, domain));
            tableHandle = applyFilter(metadata, tableHandle, new Constraint(tupleDomain));

            try {
                getSplitCount(getSplits(splitManager, transaction, session, tableHandle));
                fail("Expected PartitionOfflineException");
            }
            catch (PartitionOfflineException e) {
                assertEquals(e.getTableName(), tableOfflinePartition);
                assertEquals(e.getPartition(), "ds=2012-12-30");
            }
        }
    }

    @Test
    public void testGetPartitionSplitsTableNotReadablePartition()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableNotReadable);
            assertNotNull(tableHandle);

            try {
                getSplitCount(getSplits(splitManager, transaction, session, tableHandle));
                fail("Expected HiveNotReadableException");
            }
            catch (HiveNotReadableException e) {
                assertThat(e).hasMessageMatching("Table '.*\\.trino_test_not_readable' is not readable: reason for not readable");
                assertEquals(e.getTableName(), tableNotReadable);
                assertEquals(e.getPartition(), Optional.empty());
            }
        }
    }

    @Test
    public void testBucketedTableStringInt()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedStringInt);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            assertTableIsBucketed(tableHandle, transaction, session);

            String testString = "test";
            Integer testInt = 13;
            Short testSmallint = 12;

            // Reverse the order of bindings as compared to bucketing order
            ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                    .put(columnHandles.get(columnIndex.get("t_int")), NullableValue.of(INTEGER, (long) testInt))
                    .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(createUnboundedVarcharType(), utf8Slice(testString)))
                    .put(columnHandles.get(columnIndex.get("t_smallint")), NullableValue.of(SMALLINT, (long) testSmallint))
                    .buildOrThrow();

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

            boolean rowFound = false;
            for (MaterializedRow row : result) {
                if (testString.equals(row.getField(columnIndex.get("t_string"))) &&
                        testInt.equals(row.getField(columnIndex.get("t_int"))) &&
                        testSmallint.equals(row.getField(columnIndex.get("t_smallint")))) {
                    rowFound = true;
                }
            }
            assertTrue(rowFound);
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testBucketedTableBigintBoolean()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedBigintBoolean);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            assertTableIsBucketed(tableHandle, transaction, session);
            ConnectorTableProperties properties = metadata.getTableProperties(
                    newSession(ImmutableMap.of("propagate_table_scan_sorting_properties", true)),
                    tableHandle);
            // trino_test_bucketed_by_bigint_boolean does not define sorting, therefore local properties is empty
            assertTrue(properties.getLocalProperties().isEmpty());
            assertTrue(metadata.getTableProperties(newSession(), tableHandle).getLocalProperties().isEmpty());

            String testString = "test";
            Long testBigint = 89L;
            Boolean testBoolean = true;

            ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                    .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(createUnboundedVarcharType(), utf8Slice(testString)))
                    .put(columnHandles.get(columnIndex.get("t_bigint")), NullableValue.of(BIGINT, testBigint))
                    .put(columnHandles.get(columnIndex.get("t_boolean")), NullableValue.of(BOOLEAN, testBoolean))
                    .buildOrThrow();

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

            boolean rowFound = false;
            for (MaterializedRow row : result) {
                if (testString.equals(row.getField(columnIndex.get("t_string"))) &&
                        testBigint.equals(row.getField(columnIndex.get("t_bigint"))) &&
                        testBoolean.equals(row.getField(columnIndex.get("t_boolean")))) {
                    rowFound = true;
                    break;
                }
            }
            assertTrue(rowFound);
        }
    }

    @Test
    public void testBucketedTableDoubleFloat()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedDoubleFloat);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            assertTableIsBucketed(tableHandle, transaction, session);

            ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                    .put(columnHandles.get(columnIndex.get("t_float")), NullableValue.of(REAL, (long) floatToRawIntBits(87.1f)))
                    .put(columnHandles.get(columnIndex.get("t_double")), NullableValue.of(DOUBLE, 88.2))
                    .buildOrThrow();

            // floats and doubles are not supported, so we should see all splits
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(32), Optional.empty());
            assertEquals(result.getRowCount(), 100);
        }
    }

    @Test
    public void testBucketedTableEvolution()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryBucketEvolutionTable = temporaryTable("bucket_evolution");
            try {
                doTestBucketedTableEvolution(storageFormat, temporaryBucketEvolutionTable);
            }
            finally {
                dropTable(temporaryBucketEvolutionTable);
            }
        }
    }

    private void doTestBucketedTableEvolution(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        int rowCount = 100;

        //
        // Produce a table with 8 buckets.
        // The table has 3 partitions of 3 different bucket count (4, 8, 16).
        createEmptyTable(
                tableName,
                storageFormat,
                ImmutableList.of(
                        new Column("id", HIVE_LONG, Optional.empty()),
                        new Column("name", HIVE_STRING, Optional.empty())),
                ImmutableList.of(new Column("pk", HIVE_STRING, Optional.empty())),
                Optional.of(new HiveBucketProperty(ImmutableList.of("id"), BUCKETING_V1, 4, ImmutableList.of())));
        // write a 4-bucket partition
        MaterializedResult.Builder bucket4Builder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> bucket4Builder.row((long) i, String.valueOf(i), "four"));
        insertData(tableName, bucket4Builder.build());
        // write a 16-bucket partition
        alterBucketProperty(tableName, Optional.of(new HiveBucketProperty(ImmutableList.of("id"), BUCKETING_V1, 16, ImmutableList.of())));
        MaterializedResult.Builder bucket16Builder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> bucket16Builder.row((long) i, String.valueOf(i), "sixteen"));
        insertData(tableName, bucket16Builder.build());
        // write an 8-bucket partition
        alterBucketProperty(tableName, Optional.of(new HiveBucketProperty(ImmutableList.of("id"), BUCKETING_V1, 8, ImmutableList.of())));
        MaterializedResult.Builder bucket8Builder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> bucket8Builder.row((long) i, String.valueOf(i), "eight"));
        insertData(tableName, bucket8Builder.build());

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // read entire table
            List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>builder()
                    .addAll(metadata.getColumnHandles(session, tableHandle).values())
                    .build();
            MaterializedResult result = readTable(
                    transaction,
                    tableHandle,
                    columnHandles,
                    session,
                    TupleDomain.all(),
                    OptionalInt.empty(),
                    Optional.empty());
            assertBucketTableEvolutionResult(result, columnHandles, ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7), rowCount);

            // read single bucket (table/logical bucket)
            result = readTable(
                    transaction,
                    tableHandle,
                    columnHandles,
                    session,
                    TupleDomain.fromFixedValues(ImmutableMap.of(bucketColumnHandle(), NullableValue.of(INTEGER, 6L))),
                    OptionalInt.empty(),
                    Optional.empty());
            assertBucketTableEvolutionResult(result, columnHandles, ImmutableSet.of(6), rowCount);

            // read single bucket, without selecting the bucketing column (i.e. id column)
            columnHandles = ImmutableList.<ColumnHandle>builder()
                    .addAll(metadata.getColumnHandles(session, tableHandle).values().stream()
                            .filter(columnHandle -> !"id".equals(((HiveColumnHandle) columnHandle).getName()))
                            .collect(toImmutableList()))
                    .build();
            result = readTable(
                    transaction,
                    tableHandle,
                    columnHandles,
                    session,
                    TupleDomain.fromFixedValues(ImmutableMap.of(bucketColumnHandle(), NullableValue.of(INTEGER, 6L))),
                    OptionalInt.empty(),
                    Optional.empty());
            assertBucketTableEvolutionResult(result, columnHandles, ImmutableSet.of(6), rowCount);
        }
    }

    private static void assertBucketTableEvolutionResult(MaterializedResult result, List<ColumnHandle> columnHandles, Set<Integer> bucketIds, int rowCount)
    {
        // Assert that only elements in the specified bucket shows up, and each element shows up 3 times.
        int bucketCount = 8;
        Set<Long> expectedIds = LongStream.range(0, rowCount)
                .filter(x -> bucketIds.contains(toIntExact(x % bucketCount)))
                .boxed()
                .collect(toImmutableSet());

        // assert that content from all three buckets are the same
        Map<String, Integer> columnIndex = indexColumns(columnHandles);
        OptionalInt idColumnIndex = columnIndex.containsKey("id") ? OptionalInt.of(columnIndex.get("id")) : OptionalInt.empty();
        int nameColumnIndex = columnIndex.get("name");
        int bucketColumnIndex = columnIndex.get(BUCKET_COLUMN_NAME);
        Map<Long, Integer> idCount = new HashMap<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            String name = (String) row.getField(nameColumnIndex);
            int bucket = (int) row.getField(bucketColumnIndex);
            idCount.compute(Long.parseLong(name), (key, oldValue) -> oldValue == null ? 1 : oldValue + 1);
            assertEquals(bucket, Integer.parseInt(name) % bucketCount);
            if (idColumnIndex.isPresent()) {
                long id = (long) row.getField(idColumnIndex.getAsInt());
                assertEquals(Integer.parseInt(name), id);
            }
        }
        assertEquals(
                (int) idCount.values().stream()
                        .distinct()
                        .collect(onlyElement()),
                3);
        assertEquals(idCount.keySet(), expectedIds);
    }

    @Test
    public void testBucketedSortedTableEvolution()
            throws Exception
    {
        SchemaTableName temporaryTable = temporaryTable("test_bucket_sorting_evolution");
        try {
            doTestBucketedSortedTableEvolution(temporaryTable);
        }
        finally {
            dropTable(temporaryTable);
        }
    }

    private void doTestBucketedSortedTableEvolution(SchemaTableName tableName)
            throws Exception
    {
        int rowCount = 100;
        // Create table and populate it with 3 partitions with different sort orders but same bucketing
        createEmptyTable(
                tableName,
                ORC,
                ImmutableList.of(
                        new Column("id", HIVE_LONG, Optional.empty()),
                        new Column("name", HIVE_STRING, Optional.empty())),
                ImmutableList.of(new Column("pk", HIVE_STRING, Optional.empty())),
                Optional.of(new HiveBucketProperty(
                        ImmutableList.of("id"),
                        BUCKETING_V1,
                        4,
                        ImmutableList.of(new SortingColumn("id", ASCENDING), new SortingColumn("name", ASCENDING)))));
        // write a 4-bucket partition sorted by id, name
        MaterializedResult.Builder sortedByIdNameBuilder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> sortedByIdNameBuilder.row((long) i, String.valueOf(i), "sorted_by_id_name"));
        insertData(tableName, sortedByIdNameBuilder.build());

        // write a 4-bucket partition sorted by name
        alterBucketProperty(tableName, Optional.of(new HiveBucketProperty(
                ImmutableList.of("id"),
                BUCKETING_V1,
                4,
                ImmutableList.of(new SortingColumn("name", ASCENDING)))));
        MaterializedResult.Builder sortedByNameBuilder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> sortedByNameBuilder.row((long) i, String.valueOf(i), "sorted_by_name"));
        insertData(tableName, sortedByNameBuilder.build());

        // write a 4-bucket partition sorted by id
        alterBucketProperty(tableName, Optional.of(new HiveBucketProperty(
                ImmutableList.of("id"),
                BUCKETING_V1,
                4,
                ImmutableList.of(new SortingColumn("id", ASCENDING)))));
        MaterializedResult.Builder sortedByIdBuilder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> sortedByIdBuilder.row((long) i, String.valueOf(i), "sorted_by_id"));
        insertData(tableName, sortedByIdBuilder.build());

        ConnectorTableHandle tableHandle;
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);
            tableHandle = getTableHandle(metadata, tableName);

            // read entire table
            List<ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .collect(toImmutableList());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEquals(result.getRowCount(), 300);
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession(ImmutableMap.of("propagate_table_scan_sorting_properties", true));
            metadata.beginQuery(session);
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            // verify local sorting property
            ConnectorTableProperties properties = metadata.getTableProperties(session, tableHandle);
            assertEquals(properties.getLocalProperties(), ImmutableList.of(
                    new SortingProperty<>(columnHandles.get("id"), ASC_NULLS_FIRST)));

            // read on a entire table should fail with exception
            assertThatThrownBy(() -> readTable(transaction, tableHandle, ImmutableList.copyOf(columnHandles.values()), session, TupleDomain.all(), OptionalInt.empty(), Optional.empty()))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Hive table (%s) sorting by [id] is not compatible with partition (pk=sorted_by_name) sorting by [name]." +
                            " This restriction can be avoided by disabling propagate_table_scan_sorting_properties.", tableName);

            // read only the partitions with sorting that is compatible to table sorting
            MaterializedResult result = readTable(
                    transaction,
                    tableHandle,
                    ImmutableList.copyOf(columnHandles.values()),
                    session,
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            columnHandles.get("pk"),
                            Domain.create(ValueSet.of(VARCHAR, utf8Slice("sorted_by_id_name"), utf8Slice("sorted_by_id")), false))),
                    OptionalInt.empty(),
                    Optional.empty());
            assertEquals(result.getRowCount(), 200);
        }
    }

    @Test
    public void testBucketedTableValidation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName table = temporaryTable("bucket_validation");
            try {
                doTestBucketedTableValidation(storageFormat, table);
            }
            finally {
                dropTable(table);
            }
        }
    }

    private void doTestBucketedTableValidation(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        prepareInvalidBuckets(storageFormat, tableName);

        // read succeeds when validation is disabled
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession(ImmutableMap.of("validate_bucketing", false));
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEquals(result.getRowCount(), 87); // fewer rows due to deleted file
        }

        // read fails due to validation failure
        assertReadFailsWithMessageMatching(storageFormat, tableName, "Hive table is corrupt\\. File '.*/000002_0_.*' is for bucket 2, but contains a row for bucket 5.");
    }

    private void prepareInvalidBuckets(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        createEmptyTable(
                tableName,
                storageFormat,
                ImmutableList.of(
                        new Column("id", HIVE_LONG, Optional.empty()),
                        new Column("name", HIVE_STRING, Optional.empty())),
                ImmutableList.of(),
                Optional.of(new HiveBucketProperty(ImmutableList.of("id"), BUCKETING_V1, 8, ImmutableList.of())));

        MaterializedResult.Builder dataBuilder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR);
        for (long id = 0; id < 100; id++) {
            dataBuilder.row(id, String.valueOf(id));
        }
        insertData(tableName, dataBuilder.build());

        try (Transaction transaction = newTransaction()) {
            Set<String> files = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());

            Path bucket2 = files.stream()
                    .map(Path::new)
                    .filter(path -> path.getName().startsWith("000002_0_"))
                    .collect(onlyElement());

            Path bucket5 = files.stream()
                    .map(Path::new)
                    .filter(path -> path.getName().startsWith("000005_0_"))
                    .collect(onlyElement());

            HdfsContext context = new HdfsContext(newSession());
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, bucket2);
            fileSystem.delete(bucket2, false);
            fileSystem.rename(bucket5, bucket2);
        }
    }

    protected void assertReadFailsWithMessageMatching(HiveStorageFormat storageFormat, SchemaTableName tableName, String regex)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            assertTrinoExceptionThrownBy(
                    () -> readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat)))
                    .hasErrorCode(HIVE_INVALID_BUCKET_FILES)
                    .hasMessageMatching(regex);
        }
    }

    private void assertTableIsBucketed(ConnectorTableHandle tableHandle, Transaction transaction, ConnectorSession session)
    {
        // the bucketed test tables should have ~32 splits
        List<ConnectorSplit> splits = getAllSplits(tableHandle, transaction, session);
        assertThat(splits.size()).as("splits.size()")
                .isBetween(31, 32);

        // verify all paths are unique
        Set<String> paths = new HashSet<>();
        for (ConnectorSplit split : splits) {
            assertTrue(paths.add(((HiveSplit) split).getPath()));
        }
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorSplit> splits = getAllSplits(tableHandle, transaction, session);
            assertEquals(splits.size(), tablePartitionFormatPartitions.size());

            for (ConnectorSplit split : splits) {
                HiveSplit hiveSplit = (HiveSplit) split;

                List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
                String ds = partitionKeys.get(0).getValue();
                String fileFormat = partitionKeys.get(1).getValue();
                HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase(ENGLISH));
                int dummyPartition = Integer.parseInt(partitionKeys.get(2).getValue());

                long rowNumber = 0;
                long completedBytes = 0;
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, tableHandle, columnHandles, DynamicFilter.EMPTY)) {
                    MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                    assertPageSourceType(pageSource, fileType);

                    for (MaterializedRow row : result) {
                        try {
                            assertValueTypes(row, tableMetadata.getColumns());
                        }
                        catch (RuntimeException e) {
                            throw new RuntimeException("row " + rowNumber, e);
                        }

                        rowNumber++;
                        Object value;

                        value = row.getField(columnIndex.get("t_string"));
                        if (rowNumber % 19 == 0) {
                            assertNull(value);
                        }
                        else if (rowNumber % 19 == 1) {
                            assertEquals(value, "");
                        }
                        else {
                            assertEquals(value, "test");
                        }

                        assertEquals(row.getField(columnIndex.get("t_tinyint")), (byte) (1 + rowNumber));
                        assertEquals(row.getField(columnIndex.get("t_smallint")), (short) (2 + rowNumber));
                        assertEquals(row.getField(columnIndex.get("t_int")), 3 + (int) rowNumber);

                        if (rowNumber % 13 == 0) {
                            assertNull(row.getField(columnIndex.get("t_bigint")));
                        }
                        else {
                            assertEquals(row.getField(columnIndex.get("t_bigint")), 4 + rowNumber);
                        }

                        assertEquals((Float) row.getField(columnIndex.get("t_float")), 5.1f + rowNumber, 0.001);
                        assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);

                        if (rowNumber % 3 == 2) {
                            assertNull(row.getField(columnIndex.get("t_boolean")));
                        }
                        else {
                            assertEquals(row.getField(columnIndex.get("t_boolean")), rowNumber % 3 != 0);
                        }

                        assertEquals(row.getField(columnIndex.get("ds")), ds);
                        assertEquals(row.getField(columnIndex.get("file_format")), fileFormat);
                        assertEquals(row.getField(columnIndex.get("dummy")), dummyPartition);

                        long newCompletedBytes = pageSource.getCompletedBytes();
                        assertTrue(newCompletedBytes >= completedBytes);
                        assertTrue(newCompletedBytes <= hiveSplit.getLength());
                        completedBytes = newCompletedBytes;
                    }

                    assertTrue(completedBytes <= hiveSplit.getLength());
                    assertEquals(rowNumber, 100);
                }
            }
        }
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorSplit> splits = getAllSplits(tableHandle, transaction, session);
            assertEquals(splits.size(), tablePartitionFormatPartitions.size());

            for (ConnectorSplit split : splits) {
                HiveSplit hiveSplit = (HiveSplit) split;

                List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
                String ds = partitionKeys.get(0).getValue();
                String fileFormat = partitionKeys.get(1).getValue();
                HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase(ENGLISH));
                int dummyPartition = Integer.parseInt(partitionKeys.get(2).getValue());

                long rowNumber = 0;
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, tableHandle, columnHandles, DynamicFilter.EMPTY)) {
                    assertPageSourceType(pageSource, fileType);
                    MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                    for (MaterializedRow row : result) {
                        rowNumber++;

                        assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);
                        assertEquals(row.getField(columnIndex.get("ds")), ds);
                        assertEquals(row.getField(columnIndex.get("file_format")), fileFormat);
                        assertEquals(row.getField(columnIndex.get("dummy")), dummyPartition);
                    }
                }
                assertEquals(rowNumber, 100);
            }
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorSplit> splits = getAllSplits(tableHandle, transaction, session);
            assertThat(splits).hasSameSizeAs(tableUnpartitionedPartitions);

            for (ConnectorSplit split : splits) {
                HiveSplit hiveSplit = (HiveSplit) split;

                assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

                long rowNumber = 0;
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, tableHandle, columnHandles, DynamicFilter.EMPTY)) {
                    assertPageSourceType(pageSource, TEXTFILE);
                    MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                    for (MaterializedRow row : result) {
                        rowNumber++;

                        if (rowNumber % 19 == 0) {
                            assertNull(row.getField(columnIndex.get("t_string")));
                        }
                        else if (rowNumber % 19 == 1) {
                            assertEquals(row.getField(columnIndex.get("t_string")), "");
                        }
                        else {
                            assertEquals(row.getField(columnIndex.get("t_string")), "unpartitioned");
                        }

                        assertEquals(row.getField(columnIndex.get("t_tinyint")), (byte) (1 + rowNumber));
                    }
                }
                assertEquals(rowNumber, 100);
            }
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata connectorMetadata = transaction.getMetadata();
            ConnectorTableHandle table = getTableHandle(connectorMetadata, tableUnpartitioned);
            ConnectorSession session = newSession();
            connectorMetadata.beginQuery(session);
            readTable(transaction, table, ImmutableList.of(invalidColumnHandle), session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
        }
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.trino_test_partition_schema_change' is declared as type 'double', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
    public void testPartitionSchemaMismatch()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle table = getTableHandle(metadata, tablePartitionSchemaChange);
            ConnectorSession session = newSession();
            metadata.beginQuery(session);
            readTable(transaction, table, ImmutableList.of(dsColumn), session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
        }
    }

    // TODO coercion of non-canonical values should be supported
    @Test(enabled = false)
    public void testPartitionSchemaNonCanonical()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle table = getTableHandle(metadata, tablePartitionSchemaChangeNonCanonical);
            ColumnHandle column = metadata.getColumnHandles(session, table).get("t_boolean");

            Constraint constraint = new Constraint(TupleDomain.fromFixedValues(ImmutableMap.of(column, NullableValue.of(BOOLEAN, false))));
            table = applyFilter(metadata, table, constraint);
            HivePartition partition = getOnlyElement(((HiveTableHandle) table).getPartitions().orElseThrow(AssertionError::new));
            assertEquals(getPartitionId(partition), "t_boolean=0");

            ConnectorSplitSource splitSource = getSplits(splitManager, transaction, session, table);
            ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

            ImmutableList<ColumnHandle> columnHandles = ImmutableList.of(column);
            try (ConnectorPageSource ignored = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, table, columnHandles, DynamicFilter.EMPTY)) {
                fail("expected exception");
            }
            catch (TrinoException e) {
                assertEquals(e.getErrorCode(), HIVE_INVALID_PARTITION_VALUE.toErrorCode());
            }
        }
    }

    @Test
    public void testTypesTextFile()
            throws Exception
    {
        assertGetRecords("trino_test_types_textfile", TEXTFILE);
    }

    @Test
    public void testTypesSequenceFile()
            throws Exception
    {
        assertGetRecords("trino_test_types_sequencefile", SEQUENCEFILE);
    }

    @Test
    public void testTypesRcText()
            throws Exception
    {
        assertGetRecords("trino_test_types_rctext", RCTEXT);
    }

    @Test
    public void testTypesRcBinary()
            throws Exception
    {
        assertGetRecords("trino_test_types_rcbinary", RCBINARY);
    }

    @Test
    public void testTypesOrc()
            throws Exception
    {
        assertGetRecords("trino_test_types_orc", ORC);
    }

    @Test
    public void testTypesParquet()
            throws Exception
    {
        assertGetRecords("trino_test_types_parquet", PARQUET);
    }

    @Test
    public void testEmptyTextFile()
            throws Exception
    {
        assertEmptyFile(TEXTFILE);
    }

    @Test
    public void testEmptySequenceFile()
            throws Exception
    {
        assertEmptyFile(SEQUENCEFILE);
    }

    @Test
    public void testEmptyRcTextFile()
            throws Exception
    {
        assertEmptyFile(RCTEXT);
    }

    @Test
    public void testEmptyRcBinaryFile()
            throws Exception
    {
        assertEmptyFile(RCBINARY);
    }

    @Test
    public void testEmptyOrcFile()
            throws Exception
    {
        assertEmptyFile(ORC);
    }

    private void assertEmptyFile(HiveStorageFormat format)
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("empty_file");
        try {
            List<Column> columns = ImmutableList.of(new Column("test", HIVE_STRING, Optional.empty()));
            createEmptyTable(tableName, format, columns, ImmutableList.of());

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);

                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                Table table = transaction.getMetastore()
                        .getTable(tableName.getSchemaName(), tableName.getTableName())
                        .orElseThrow(AssertionError::new);

                // verify directory is empty
                HdfsContext context = new HdfsContext(session);
                Path location = new Path(table.getStorage().getLocation());
                assertTrue(listDirectory(context, location).isEmpty());

                // read table with empty directory
                readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.of(0), Optional.of(ORC));

                // create empty file
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, location);
                assertTrue(fileSystem.createNewFile(new Path(location, "empty-file")));
                assertEquals(listDirectory(context, location), ImmutableList.of("empty-file"));

                // read table with empty file
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.of(0), Optional.empty());
                assertEquals(result.getRowCount(), 0);
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testHiveViewsHaveNoColumns()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertEquals(listTableColumns(metadata, newSession(), new SchemaTablePrefix(view.getSchemaName(), view.getTableName())), ImmutableMap.of());
        }
    }

    @Test
    public void testRenameTable()
    {
        SchemaTableName temporaryRenameTableOld = temporaryTable("rename_old");
        SchemaTableName temporaryRenameTableNew = temporaryTable("rename_new");
        try {
            createDummyTable(temporaryRenameTableOld);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                metadata.renameTable(session, getTableHandle(metadata, temporaryRenameTableOld), temporaryRenameTableNew);
                transaction.commit();
            }

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                assertNull(metadata.getTableHandle(session, temporaryRenameTableOld));
                assertNotNull(metadata.getTableHandle(session, temporaryRenameTableNew));
            }
        }
        finally {
            dropTable(temporaryRenameTableOld);
            dropTable(temporaryRenameTableNew);
        }
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryCreateTable = temporaryTable("create");
            try {
                doCreateTable(temporaryCreateTable, storageFormat);
            }
            finally {
                dropTable(temporaryCreateTable);
            }
        }
    }

    @Test
    public void testTableCreationRollback()
            throws Exception
    {
        SchemaTableName temporaryCreateRollbackTable = temporaryTable("create_rollback");
        try {
            Path stagingPathRoot;
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                // begin creating the table
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateRollbackTable, CREATE_TABLE_COLUMNS, createTableProperties(RCBINARY));

                ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);

                // write the data
                ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
                sink.appendPage(CREATE_TABLE_DATA.toPage());
                getFutureValue(sink.finish());

                // verify we have data files
                stagingPathRoot = getStagingPathRoot(outputHandle);
                HdfsContext context = new HdfsContext(session);
                assertFalse(listAllDataFiles(context, stagingPathRoot).isEmpty());

                // rollback the table
                transaction.rollback();
            }

            // verify all files have been deleted
            HdfsContext context = new HdfsContext(newSession());
            assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());

            // verify table is not in the metastore
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                assertNull(metadata.getTableHandle(session, temporaryCreateRollbackTable));
            }
        }
        finally {
            dropTable(temporaryCreateRollbackTable);
        }
    }

    @Test
    public void testTableCreationIgnoreExisting()
    {
        List<Column> columns = ImmutableList.of(new Column("dummy", HiveType.valueOf("uniontype<smallint,tinyint>"), Optional.empty()));
        SchemaTableName schemaTableName = temporaryTable("create");
        ConnectorSession session = newSession();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        PrincipalPrivileges privileges = testingPrincipalPrivilege(session);
        Path targetPath;
        try {
            try (Transaction transaction = newTransaction()) {
                LocationService locationService = getLocationService();
                LocationHandle locationHandle = locationService.forNewTable(transaction.getMetastore(), session, schemaName, tableName, Optional.empty());
                targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath, "q1");
                transaction.getMetastore()
                        .createTable(session, table, privileges, Optional.empty(), Optional.empty(), false, EMPTY_TABLE_STATISTICS, false);
                Optional<Table> tableHandle = transaction.getMetastore().getTable(schemaName, tableName);
                assertTrue(tableHandle.isPresent());
                transaction.commit();
            }

            // try creating it again from another transaction with ignoreExisting=false
            try (Transaction transaction = newTransaction()) {
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath.suffix("_2"), "q2");
                transaction.getMetastore()
                        .createTable(session, table, privileges, Optional.empty(), Optional.empty(), false, EMPTY_TABLE_STATISTICS, false);
                transaction.commit();
                fail("Expected exception");
            }
            catch (TrinoException e) {
                assertInstanceOf(e, TableAlreadyExistsException.class);
            }

            // try creating it again from another transaction with ignoreExisting=true
            try (Transaction transaction = newTransaction()) {
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath.suffix("_3"), "q3");
                transaction.getMetastore()
                        .createTable(session, table, privileges, Optional.empty(), Optional.empty(), true, EMPTY_TABLE_STATISTICS, false);
                transaction.commit();
            }

            // at this point the table should exist, now try creating the table again with a different table definition
            columns = ImmutableList.of(new Column("new_column", HiveType.valueOf("string"), Optional.empty()));
            try (Transaction transaction = newTransaction()) {
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath.suffix("_4"), "q4");
                transaction.getMetastore()
                        .createTable(session, table, privileges, Optional.empty(), Optional.empty(), true, EMPTY_TABLE_STATISTICS, false);
                transaction.commit();
                fail("Expected exception");
            }
            catch (TrinoException e) {
                assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
                assertEquals(e.getMessage(), format("Table already exists with a different schema: '%s'", schemaTableName.getTableName()));
            }
        }
        finally {
            dropTable(schemaTableName);
        }
    }

    private static Table createSimpleTable(SchemaTableName schemaTableName, List<Column> columns, ConnectorSession session, Path targetPath, String queryId)
    {
        String tableOwner = session.getUser();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(Optional.of(tableOwner))
                .setTableType(TableType.MANAGED_TABLE.name())
                .setParameters(ImmutableMap.of(
                        PRESTO_VERSION_NAME, TEST_SERVER_VERSION,
                        PRESTO_QUERY_ID_NAME, queryId))
                .setDataColumns(columns)
                .withStorage(storage -> storage
                        .setLocation(targetPath.toString())
                        .setStorageFormat(fromHiveStorageFormat(ORC))
                        .setSerdeParameters(ImmutableMap.of()))
                .build();
    }

    @Test
    public void testBucketSortedTables()
            throws Exception
    {
        SchemaTableName table = temporaryTable("create_sorted");
        try {
            doTestBucketSortedTables(table);
        }
        finally {
            dropTable(table);
        }
    }

    private void doTestBucketSortedTables(SchemaTableName table)
            throws IOException
    {
        int bucketCount = 3;
        int expectedRowCount = 0;

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                    table,
                    ImmutableList.<ColumnMetadata>builder()
                            .add(new ColumnMetadata("id", VARCHAR))
                            .add(new ColumnMetadata("value_asc", VARCHAR))
                            .add(new ColumnMetadata("value_desc", BIGINT))
                            .add(new ColumnMetadata("ds", VARCHAR))
                            .build(),
                    ImmutableMap.<String, Object>builder()
                            .put(STORAGE_FORMAT_PROPERTY, RCBINARY)
                            .put(PARTITIONED_BY_PROPERTY, ImmutableList.of("ds"))
                            .put(BUCKETED_BY_PROPERTY, ImmutableList.of("id"))
                            .put(BUCKET_COUNT_PROPERTY, bucketCount)
                            .put(SORTED_BY_PROPERTY, ImmutableList.builder()
                                    .add(new SortingColumn("value_asc", ASCENDING))
                                    .add(new SortingColumn("value_desc", DESCENDING))
                                    .build())
                            .buildOrThrow());

            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);

            // write the data
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
            List<Type> types = tableMetadata.getColumns().stream()
                    .map(ColumnMetadata::getType)
                    .collect(toList());
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < 50; i++) {
                MaterializedResult.Builder builder = MaterializedResult.resultBuilder(session, types);
                for (int j = 0; j < 1000; j++) {
                    builder.row(
                            sha256().hashLong(random.nextLong()).toString(),
                            "test" + random.nextInt(100),
                            random.nextLong(100_000),
                            "2018-04-01");
                    expectedRowCount++;
                }
                sink.appendPage(builder.build().toPage());
            }

            HdfsContext context = new HdfsContext(session);
            // verify we have enough temporary files per bucket to require multiple passes
            Path stagingPathRoot;
            if (isTemporaryStagingDirectoryEnabled(session)) {
                stagingPathRoot = new Path(getTemporaryStagingDirectoryPath(session)
                        .replace("${USER}", context.getIdentity().getUser()));
            }
            else {
                stagingPathRoot = getStagingPathRoot(outputHandle);
            }

            assertThat(listAllDataFiles(context, stagingPathRoot))
                    .filteredOn(file -> file.contains(".tmp-sort."))
                    .size().isGreaterThan(bucketCount * getHiveConfig().getMaxOpenSortFiles() * 2);

            // finish the write
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // verify there are no temporary files
            for (String file : listAllDataFiles(context, stagingPathRoot)) {
                assertThat(file).doesNotContain(".tmp-sort.");
            }

            // finish creating table
            metadata.finishCreateTable(session, outputHandle, fragments, ImmutableList.of());

            transaction.commit();
        }

        // verify that bucket files are sorted
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, table);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // verify local sorting property
            ConnectorTableProperties properties = metadata.getTableProperties(
                    newSession(ImmutableMap.of(
                            "propagate_table_scan_sorting_properties", true,
                            "bucket_execution_enabled", false)),
                    tableHandle);
            Map<String, Integer> columnIndex = indexColumns(columnHandles);
            assertEquals(properties.getLocalProperties(), ImmutableList.of(
                    new SortingProperty<>(columnHandles.get(columnIndex.get("value_asc")), ASC_NULLS_FIRST),
                    new SortingProperty<>(columnHandles.get(columnIndex.get("value_desc")), DESC_NULLS_LAST)));
            assertThat(metadata.getTableProperties(newSession(), tableHandle).getLocalProperties()).isEmpty();

            List<ConnectorSplit> splits = getAllSplits(tableHandle, transaction, session);
            assertThat(splits).hasSize(bucketCount);

            int actualRowCount = 0;
            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, tableHandle, columnHandles, DynamicFilter.EMPTY)) {
                    String lastValueAsc = null;
                    long lastValueDesc = -1;

                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (page == null) {
                            continue;
                        }
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            Block blockAsc = page.getBlock(1);
                            Block blockDesc = page.getBlock(2);
                            assertFalse(blockAsc.isNull(i));
                            assertFalse(blockDesc.isNull(i));

                            String valueAsc = VARCHAR.getSlice(blockAsc, i).toStringUtf8();
                            if (lastValueAsc != null) {
                                assertGreaterThanOrEqual(valueAsc, lastValueAsc);
                                if (valueAsc.equals(lastValueAsc)) {
                                    long valueDesc = BIGINT.getLong(blockDesc, i);
                                    if (lastValueDesc != -1) {
                                        assertLessThanOrEqual(valueDesc, lastValueDesc);
                                    }
                                    lastValueDesc = valueDesc;
                                }
                                else {
                                    lastValueDesc = -1;
                                }
                            }
                            lastValueAsc = valueAsc;
                            actualRowCount++;
                        }
                    }
                }
            }
            assertThat(actualRowCount).isEqualTo(expectedRowCount);
        }
    }

    @Test
    public void testInsert()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryInsertTable = temporaryTable("insert");
            try {
                doInsert(storageFormat, temporaryInsertTable);
            }
            finally {
                dropTable(temporaryInsertTable);
            }
        }
    }

    @Test
    public void testInsertOverwriteUnpartitioned()
            throws Exception
    {
        SchemaTableName table = temporaryTable("insert_overwrite");
        try {
            doInsertOverwriteUnpartitioned(table);
        }
        finally {
            dropTable(table);
        }
    }

    @Test
    public void testInsertIntoNewPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryInsertIntoNewPartitionTable = temporaryTable("insert_new_partitioned");
            try {
                doInsertIntoNewPartition(storageFormat, temporaryInsertIntoNewPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoNewPartitionTable);
            }
        }
    }

    @Test
    public void testInsertIntoExistingPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryInsertIntoExistingPartitionTable = temporaryTable("insert_existing_partitioned");
            try {
                doInsertIntoExistingPartition(storageFormat, temporaryInsertIntoExistingPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoExistingPartitionTable);
            }
        }
    }

    @Test
    public void testInsertIntoExistingPartitionEmptyStatistics()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryInsertIntoExistingPartitionTable = temporaryTable("insert_existing_partitioned_empty_statistics");
            try {
                doInsertIntoExistingPartitionEmptyStatistics(storageFormat, temporaryInsertIntoExistingPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoExistingPartitionTable);
            }
        }
    }

    @Test
    public void testInsertUnsupportedWriteType()
            throws Exception
    {
        SchemaTableName temporaryInsertUnsupportedWriteType = temporaryTable("insert_unsupported_type");
        try {
            doInsertUnsupportedWriteType(ORC, temporaryInsertUnsupportedWriteType);
        }
        finally {
            dropTable(temporaryInsertUnsupportedWriteType);
        }
    }

    @Test
    public void testMetadataDelete()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryMetadataDeleteTable = temporaryTable("metadata_delete");
            try {
                doTestMetadataDelete(storageFormat, temporaryMetadataDeleteTable);
            }
            finally {
                dropTable(temporaryMetadataDeleteTable);
            }
        }
    }

    @Test
    public void testEmptyTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryCreateEmptyTable = temporaryTable("create_empty");
            try {
                doCreateEmptyTable(temporaryCreateEmptyTable, storageFormat, CREATE_TABLE_COLUMNS);
            }
            finally {
                dropTable(temporaryCreateEmptyTable);
            }
        }
    }

    @Test
    public void testViewCreation()
    {
        SchemaTableName temporaryCreateView = temporaryTable("create_view");
        try {
            verifyViewCreation(temporaryCreateView);
        }
        finally {
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.dropView(newSession(), temporaryCreateView);
                transaction.commit();
            }
            catch (RuntimeException e) {
                // this usually occurs because the view was not created
            }
        }
    }

    @Test
    public void testCreateTableUnsupportedType()
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", HYPER_LOG_LOG));
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(invalidTable, columns, createTableProperties(storageFormat));
                metadata.beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);
                fail("create table with unsupported type should fail for storage format " + storageFormat);
            }
            catch (TrinoException e) {
                assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            }
        }
    }

    @Test
    public void testHideDeltaLakeTables()
    {
        ConnectorSession session = newSession();
        SchemaTableName tableName = temporaryTable("trino_delta_lake_table");

        Table.Builder table = Table.builder()
                .setDatabaseName(tableName.getSchemaName())
                .setTableName(tableName.getTableName())
                .setOwner(Optional.of(session.getUser()))
                .setTableType(MANAGED_TABLE.name())
                .setPartitionColumns(List.of(new Column("a_partition_column", HIVE_INT, Optional.empty())))
                .setDataColumns(List.of(new Column("a_column", HIVE_STRING, Optional.empty())))
                .setParameter(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER);
        table.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(PARQUET))
                .setLocation(getTableDefaultLocation(
                        metastoreClient.getDatabase(tableName.getSchemaName()).orElseThrow(),
                        new HdfsContext(session.getIdentity()),
                        hdfsEnvironment,
                        tableName.getSchemaName(),
                        tableName.getTableName()).toString());
        metastoreClient.createTable(table.build(), NO_PRIVILEGES);

        try {
            // Verify the table was created as a Delta Lake table
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);
                assertThatThrownBy(() -> getTableHandle(metadata, tableName))
                        .hasMessage(format("Cannot query Delta Lake table '%s'", tableName));
            }

            // Verify the hidden `$properties` and `$partitions` Delta Lake table handle can't be obtained within the hive connector
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);
                SchemaTableName propertiesTableName = new SchemaTableName(tableName.getSchemaName(), format("%s$properties", tableName.getTableName()));
                assertThat(metadata.getSystemTable(newSession(), propertiesTableName)).isEmpty();
                SchemaTableName partitionsTableName = new SchemaTableName(tableName.getSchemaName(), format("%s$partitions", tableName.getTableName()));
                assertThat(metadata.getSystemTable(newSession(), partitionsTableName)).isEmpty();
            }

            // Assert that table is hidden
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();

                // TODO (https://github.com/trinodb/trino/issues/5426) these assertions should use information_schema instead of metadata directly,
                //  as information_schema or MetadataManager may apply additional logic

                // list all tables
                assertThat(metadata.listTables(session, Optional.empty()))
                        .doesNotContain(tableName);

                // list all tables in a schema
                assertThat(metadata.listTables(session, Optional.of(tableName.getSchemaName())))
                        .doesNotContain(tableName);

                // list all columns
                assertThat(listTableColumns(metadata, session, new SchemaTablePrefix()).keySet())
                        .doesNotContain(tableName);

                // list all columns in a schema
                assertThat(listTableColumns(metadata, session, new SchemaTablePrefix(tableName.getSchemaName())).keySet())
                        .doesNotContain(tableName);

                // list all columns in a table
                assertThat(listTableColumns(metadata, session, new SchemaTablePrefix(tableName.getSchemaName(), tableName.getTableName())).keySet())
                        .doesNotContain(tableName);
            }
        }
        finally {
            // Clean up
            metastoreClient.dropTable(tableName.getSchemaName(), tableName.getTableName(), true);
        }
    }

    @Test
    public void testDisallowQueryingOfIcebergTables()
    {
        ConnectorSession session = newSession();
        SchemaTableName tableName = temporaryTable("trino_iceberg_table");

        Table.Builder table = Table.builder()
                .setDatabaseName(tableName.getSchemaName())
                .setTableName(tableName.getTableName())
                .setOwner(Optional.of(session.getUser()))
                .setTableType(MANAGED_TABLE.name())
                .setPartitionColumns(List.of(new Column("a_partition_column", HIVE_INT, Optional.empty())))
                .setDataColumns(List.of(new Column("a_column", HIVE_STRING, Optional.empty())))
                .setParameter(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE);
        table.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(PARQUET))
                .setLocation(getTableDefaultLocation(
                        metastoreClient.getDatabase(tableName.getSchemaName()).orElseThrow(),
                        new HdfsContext(session.getIdentity()),
                        hdfsEnvironment,
                        tableName.getSchemaName(),
                        tableName.getTableName()).toString());
        metastoreClient.createTable(table.build(), NO_PRIVILEGES);

        try {
            // Verify that the table was created as a Iceberg table can't be queried in hive
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);
                assertThatThrownBy(() -> getTableHandle(metadata, tableName))
                        .hasMessage(format("Cannot query Iceberg table '%s'", tableName));
            }

            // Verify the hidden `$properties` and `$partitions` hive system tables table handle can't be obtained for the Iceberg tables
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);
                SchemaTableName propertiesTableName = new SchemaTableName(tableName.getSchemaName(), format("%s$properties", tableName.getTableName()));
                assertThat(metadata.getSystemTable(newSession(), propertiesTableName)).isEmpty();
                SchemaTableName partitionsTableName = new SchemaTableName(tableName.getSchemaName(), format("%s$partitions", tableName.getTableName()));
                assertThat(metadata.getSystemTable(newSession(), partitionsTableName)).isEmpty();
            }
        }
        finally {
            // Clean up
            metastoreClient.dropTable(tableName.getSchemaName(), tableName.getTableName(), true);
        }
    }

    @Test
    public void testUpdateBasicTableStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_basic_table_statistics");
        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, BASIC_STATISTICS_1, BASIC_STATISTICS_2);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateTableColumnStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_table_column_statistics");
        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, STATISTICS_1_1, STATISTICS_1_2, STATISTICS_2);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_table_column_statistics_empty_optional_fields");
        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, STATISTICS_EMPTY_OPTIONAL_FIELDS);
        }
        finally {
            dropTable(tableName);
        }
    }

    protected void testUpdateTableStatistics(SchemaTableName tableName, PartitionStatistics initialStatistics, PartitionStatistics... statistics)
    {
        HiveMetastoreClosure metastoreClient = new HiveMetastoreClosure(getMetastoreClient());
        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(initialStatistics);

        AtomicReference<PartitionStatistics> expectedStatistics = new AtomicReference<>(initialStatistics);
        for (PartitionStatistics partitionStatistics : statistics) {
            metastoreClient.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), NO_ACID_TRANSACTION, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatistics.get());
                return partitionStatistics;
            });
            assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                    .isEqualTo(partitionStatistics);
            expectedStatistics.set(partitionStatistics);
        }

        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(expectedStatistics.get());

        metastoreClient.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), NO_ACID_TRANSACTION, actualStatistics -> {
            assertThat(actualStatistics).isEqualTo(expectedStatistics.get());
            return initialStatistics;
        });

        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(initialStatistics);
    }

    @Test
    public void testUpdateBasicPartitionStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_basic_partition_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(BASIC_STATISTICS_1, BASIC_STATISTICS_2),
                    ImmutableList.of(BASIC_STATISTICS_2, BASIC_STATISTICS_1));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdatePartitionColumnStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(STATISTICS_1_1, STATISTICS_1_2, STATISTICS_2),
                    ImmutableList.of(STATISTICS_1_2, STATISTICS_1_1, STATISTICS_2));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(STATISTICS_EMPTY_OPTIONAL_FIELDS),
                    ImmutableList.of(STATISTICS_EMPTY_OPTIONAL_FIELDS));
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * During table scan, the illegal storage format for some specific table should not fail the whole table scan
     */
    @Test
    public void testIllegalStorageFormatDuringTableScan()
    {
        SchemaTableName schemaTableName = temporaryTable("test_illegal_storage_format");
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            List<Column> columns = ImmutableList.of(new Column("pk", HIVE_STRING, Optional.empty()));
            String tableOwner = session.getUser();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();
            LocationHandle locationHandle = locationService.forNewTable(transaction.getMetastore(), session, schemaName, tableName, Optional.empty());
            Path targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
            //create table whose storage format is null
            Table.Builder tableBuilder = Table.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setOwner(Optional.of(tableOwner))
                    .setTableType(TableType.MANAGED_TABLE.name())
                    .setParameters(ImmutableMap.of(
                            PRESTO_VERSION_NAME, TEST_SERVER_VERSION,
                            PRESTO_QUERY_ID_NAME, session.getQueryId()))
                    .setDataColumns(columns)
                    .withStorage(storage -> storage
                            .setLocation(targetPath.toString())
                            .setStorageFormat(StorageFormat.createNullable(null, null, null))
                            .setSerdeParameters(ImmutableMap.of()));
            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(tableOwner, session.getUser());
            transaction.getMetastore().createTable(session, tableBuilder.build(), principalPrivileges, Optional.empty(), Optional.empty(), true, EMPTY_TABLE_STATISTICS, false);
            transaction.commit();
        }

        // We retrieve the table whose storageFormat has null serde/inputFormat/outputFormat
        // to make sure it can still be retrieved instead of throwing exception.
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> allColumns = listTableColumns(metadata, newSession(), new SchemaTablePrefix(schemaTableName.getSchemaName()));
            assertTrue(allColumns.containsKey(schemaTableName));
        }
        finally {
            dropTable(schemaTableName);
        }
    }

    private static Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorMetadata metadata, ConnectorSession session, SchemaTablePrefix prefix)
    {
        return metadata.streamTableColumns(session, prefix)
                .collect(toImmutableMap(
                        TableColumnsMetadata::getTable,
                        tableColumns -> tableColumns.getColumns().orElseThrow(() -> new IllegalStateException("Table " + tableColumns.getTable() + " reported as redirected"))));
    }

    private void createDummyTable(SchemaTableName tableName)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", createUnboundedVarcharType()));
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(TEXTFILE));
            ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);
            metadata.finishCreateTable(session, handle, ImmutableList.of(), ImmutableList.of());

            transaction.commit();
        }
    }

    protected void createDummyPartitionedTable(SchemaTableName tableName, List<ColumnMetadata> columns)
            throws Exception
    {
        doCreateEmptyTable(tableName, ORC, columns);

        HiveMetastoreClosure metastoreClient = new HiveMetastoreClosure(getMetastoreClient());
        Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        List<String> firstPartitionValues = ImmutableList.of("2016-01-01");
        List<String> secondPartitionValues = ImmutableList.of("2016-01-02");

        String firstPartitionName = makePartName(ImmutableList.of("ds"), firstPartitionValues);
        String secondPartitionName = makePartName(ImmutableList.of("ds"), secondPartitionValues);

        List<PartitionWithStatistics> partitions = ImmutableList.of(firstPartitionName, secondPartitionName)
                .stream()
                .map(partitionName -> new PartitionWithStatistics(createDummyPartition(table, partitionName), partitionName, PartitionStatistics.empty()))
                .collect(toImmutableList());
        metastoreClient.addPartitions(tableName.getSchemaName(), tableName.getTableName(), partitions);
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);
    }

    protected void testUpdatePartitionStatistics(
            SchemaTableName tableName,
            PartitionStatistics initialStatistics,
            List<PartitionStatistics> firstPartitionStatistics,
            List<PartitionStatistics> secondPartitionStatistics)
    {
        verify(firstPartitionStatistics.size() == secondPartitionStatistics.size());

        String firstPartitionName = "ds=2016-01-01";
        String secondPartitionName = "ds=2016-01-02";

        HiveMetastoreClosure metastoreClient = new HiveMetastoreClosure(getMetastoreClient());
        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, initialStatistics, secondPartitionName, initialStatistics));

        AtomicReference<PartitionStatistics> expectedStatisticsPartition1 = new AtomicReference<>(initialStatistics);
        AtomicReference<PartitionStatistics> expectedStatisticsPartition2 = new AtomicReference<>(initialStatistics);

        for (int i = 0; i < firstPartitionStatistics.size(); i++) {
            PartitionStatistics statisticsPartition1 = firstPartitionStatistics.get(i);
            PartitionStatistics statisticsPartition2 = secondPartitionStatistics.get(i);
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatisticsPartition1.get());
                return statisticsPartition1;
            });
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatisticsPartition2.get());
                return statisticsPartition2;
            });
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                    .isEqualTo(ImmutableMap.of(firstPartitionName, statisticsPartition1, secondPartitionName, statisticsPartition2));
            expectedStatisticsPartition1.set(statisticsPartition1);
            expectedStatisticsPartition2.set(statisticsPartition2);
        }

        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, expectedStatisticsPartition1.get(), secondPartitionName, expectedStatisticsPartition2.get()));
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, currentStatistics -> {
            assertThat(currentStatistics).isEqualTo(expectedStatisticsPartition1.get());
            return initialStatistics;
        });
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, currentStatistics -> {
            assertThat(currentStatistics).isEqualTo(expectedStatisticsPartition2.get());
            return initialStatistics;
        });
        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, initialStatistics, secondPartitionName, initialStatistics));
    }

    @Test
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, STATISTICS_1, STATISTICS_2, STATISTICS_1_1, EMPTY_TABLE_STATISTICS);
    }

    protected void testStorePartitionWithStatistics(
            List<ColumnMetadata> columns,
            PartitionStatistics statsForAllColumns1,
            PartitionStatistics statsForAllColumns2,
            PartitionStatistics statsForSubsetOfColumns,
            PartitionStatistics emptyStatistics)
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("store_partition_with_statistics");
        try {
            doCreateEmptyTable(tableName, ORC, columns);

            HiveMetastoreClosure metastoreClient = new HiveMetastoreClosure(getMetastoreClient());
            Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(tableName));

            List<String> partitionValues = ImmutableList.of("2016-01-01");
            String partitionName = makePartName(ImmutableList.of("ds"), partitionValues);

            Partition partition = createDummyPartition(table, partitionName);

            // create partition with stats for all columns
            metastoreClient.addPartitions(tableName.getSchemaName(), tableName.getTableName(), ImmutableList.of(new PartitionWithStatistics(partition, partitionName, statsForAllColumns1)));
            assertEquals(
                    metastoreClient.getPartition(tableName.getSchemaName(), tableName.getTableName(), partitionValues).get().getStorage().getStorageFormat(),
                    fromHiveStorageFormat(ORC));
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(partitionName)))
                    .isEqualTo(ImmutableMap.of(partitionName, statsForAllColumns1));

            // alter the partition into one with other stats
            Partition modifiedPartition = Partition.builder(partition)
                    .withStorage(storage -> storage
                            .setStorageFormat(fromHiveStorageFormat(RCBINARY))
                            .setLocation(partitionTargetPath(tableName, partitionName)))
                    .build();
            metastoreClient.alterPartition(tableName.getSchemaName(), tableName.getTableName(), new PartitionWithStatistics(modifiedPartition, partitionName, statsForAllColumns2));
            assertEquals(
                    metastoreClient.getPartition(tableName.getSchemaName(), tableName.getTableName(), partitionValues).get().getStorage().getStorageFormat(),
                    fromHiveStorageFormat(RCBINARY));
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(partitionName)))
                    .isEqualTo(ImmutableMap.of(partitionName, statsForAllColumns2));

            // alter the partition into one with stats for only subset of columns
            modifiedPartition = Partition.builder(partition)
                    .withStorage(storage -> storage
                            .setStorageFormat(fromHiveStorageFormat(TEXTFILE))
                            .setLocation(partitionTargetPath(tableName, partitionName)))
                    .build();
            metastoreClient.alterPartition(tableName.getSchemaName(), tableName.getTableName(), new PartitionWithStatistics(modifiedPartition, partitionName, statsForSubsetOfColumns));
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(partitionName)))
                    .isEqualTo(ImmutableMap.of(partitionName, statsForSubsetOfColumns));

            // alter the partition into one without stats
            modifiedPartition = Partition.builder(partition)
                    .withStorage(storage -> storage
                            .setStorageFormat(fromHiveStorageFormat(TEXTFILE))
                            .setLocation(partitionTargetPath(tableName, partitionName)))
                    .build();
            metastoreClient.alterPartition(tableName.getSchemaName(), tableName.getTableName(), new PartitionWithStatistics(modifiedPartition, partitionName, emptyStatistics));
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(partitionName)))
                    .isEqualTo(ImmutableMap.of(partitionName, emptyStatistics));
        }
        finally {
            dropTable(tableName);
        }
    }

    protected Partition createDummyPartition(Table table, String partitionName)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(toPartitionValues(partitionName))
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(ORC))
                        .setLocation(partitionTargetPath(new SchemaTableName(table.getDatabaseName(), table.getTableName()), partitionName)))
                .setParameters(ImmutableMap.of(
                        PRESTO_VERSION_NAME, "testversion",
                        PRESTO_QUERY_ID_NAME, "20180101_123456_00001_x1y2z"))
                .build();
    }

    protected String partitionTargetPath(SchemaTableName schemaTableName, String partitionName)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            SemiTransactionalHiveMetastore metastore = transaction.getMetastore();
            LocationService locationService = getLocationService();
            Table table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName()).get();
            LocationHandle handle = locationService.forExistingTable(metastore, session, table);
            return locationService.getPartitionWriteInfo(handle, Optional.empty(), partitionName).getTargetPath().toString();
        }
    }

    /**
     * This test creates 2 identical partitions and verifies that the statistics projected based on
     * a single partition sample are equal to the statistics computed in a fair way
     */
    @Test
    public void testPartitionStatisticsSampling()
            throws Exception
    {
        testPartitionStatisticsSampling(STATISTICS_PARTITIONED_TABLE_COLUMNS, STATISTICS_1);
    }

    protected void testPartitionStatisticsSampling(List<ColumnMetadata> columns, PartitionStatistics statistics)
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("test_partition_statistics_sampling");

        try {
            createDummyPartitionedTable(tableName, columns);
            HiveMetastoreClosure metastoreClient = new HiveMetastoreClosure(getMetastoreClient());
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), "ds=2016-01-01", actualStatistics -> statistics);
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), "ds=2016-01-02", actualStatistics -> statistics);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                ConnectorTableHandle tableHandle = metadata.getTableHandle(session, tableName);
                TableStatistics unsampledStatistics = metadata.getTableStatistics(sampleSize(2), tableHandle, Constraint.alwaysTrue());
                TableStatistics sampledStatistics = metadata.getTableStatistics(sampleSize(1), tableHandle, Constraint.alwaysTrue());
                assertEquals(sampledStatistics, unsampledStatistics);
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testApplyProjection()
            throws Exception
    {
        ColumnMetadata bigIntColumn0 = new ColumnMetadata("int0", BIGINT);
        ColumnMetadata bigIntColumn1 = new ColumnMetadata("int1", BIGINT);

        RowType oneLevelRowType = toRowType(ImmutableList.of(bigIntColumn0, bigIntColumn1));
        ColumnMetadata oneLevelRow0 = new ColumnMetadata("onelevelrow0", oneLevelRowType);

        RowType twoLevelRowType = toRowType(ImmutableList.of(oneLevelRow0, bigIntColumn0, bigIntColumn1));
        ColumnMetadata twoLevelRow0 = new ColumnMetadata("twolevelrow0", twoLevelRowType);

        List<ColumnMetadata> columnsForApplyProjectionTest = ImmutableList.of(bigIntColumn0, bigIntColumn1, oneLevelRow0, twoLevelRow0);

        SchemaTableName tableName = temporaryTable("apply_projection_tester");
        doCreateEmptyTable(tableName, ORC, columnsForApplyProjectionTest);

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            List<ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toList());
            assertEquals(columnHandles.size(), columnsForApplyProjectionTest.size());

            Map<String, ColumnHandle> columnHandleMap = columnHandles.stream()
                    .collect(toImmutableMap(handle -> ((HiveColumnHandle) handle).getBaseColumnName(), Function.identity()));

            // Emulate symbols coming from the query plan and map them to column handles
            Map<String, ColumnHandle> columnHandlesWithSymbols = ImmutableMap.of(
                    "symbol_0", columnHandleMap.get("int0"),
                    "symbol_1", columnHandleMap.get("int1"),
                    "symbol_2", columnHandleMap.get("onelevelrow0"),
                    "symbol_3", columnHandleMap.get("twolevelrow0"));

            // Create variables for the emulated symbols
            Map<String, Variable> symbolVariableMapping = columnHandlesWithSymbols.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            e -> new Variable(
                                    e.getKey(),
                                    ((HiveColumnHandle) e.getValue()).getBaseType())));

            // Create dereference expressions for testing
            FieldDereference symbol2Field0 = new FieldDereference(BIGINT, symbolVariableMapping.get("symbol_2"), 0);
            FieldDereference symbol3Field0 = new FieldDereference(oneLevelRowType, symbolVariableMapping.get("symbol_3"), 0);
            FieldDereference symbol3Field0Field0 = new FieldDereference(BIGINT, symbol3Field0, 0);
            FieldDereference symbol3Field1 = new FieldDereference(BIGINT, symbolVariableMapping.get("symbol_3"), 1);

            Map<String, ColumnHandle> inputAssignments;
            List<ConnectorExpression> inputProjections;
            Optional<ProjectionApplicationResult<ConnectorTableHandle>> projectionResult;
            List<ConnectorExpression> expectedProjections;
            Map<String, Type> expectedAssignments;

            // Test projected columns pushdown to HiveTableHandle in case of all variable references
            inputAssignments = getColumnHandlesFor(columnHandlesWithSymbols, ImmutableList.of("symbol_0", "symbol_1"));
            inputProjections = ImmutableList.of(symbolVariableMapping.get("symbol_0"), symbolVariableMapping.get("symbol_1"));
            expectedAssignments = ImmutableMap.of(
                    "symbol_0", BIGINT,
                    "symbol_1", BIGINT);
            projectionResult = metadata.applyProjection(session, tableHandle, inputProjections, inputAssignments);
            assertProjectionResult(projectionResult, false, inputProjections, expectedAssignments);

            // Empty result when projected column handles are same as those present in table handle
            projectionResult = metadata.applyProjection(session, projectionResult.get().getHandle(), inputProjections, inputAssignments);
            assertProjectionResult(projectionResult, true, ImmutableList.of(), ImmutableMap.of());

            // Extra columns handles in HiveTableHandle should get pruned
            projectionResult = metadata.applyProjection(
                    session,
                    ((HiveTableHandle) tableHandle).withProjectedColumns(ImmutableSet.copyOf(columnHandles)),
                    inputProjections,
                    inputAssignments);
            assertProjectionResult(projectionResult, false, inputProjections, expectedAssignments);

            // Test projection pushdown for dereferences
            inputAssignments = getColumnHandlesFor(columnHandlesWithSymbols, ImmutableList.of("symbol_2", "symbol_3"));
            inputProjections = ImmutableList.of(symbol2Field0, symbol3Field0Field0, symbol3Field1);
            expectedAssignments = ImmutableMap.of(
                    "onelevelrow0#f_int0", BIGINT,
                    "twolevelrow0#f_onelevelrow0#f_int0", BIGINT,
                    "twolevelrow0#f_int0", BIGINT);
            expectedProjections = ImmutableList.of(
                    new Variable("onelevelrow0#f_int0", BIGINT),
                    new Variable("twolevelrow0#f_onelevelrow0#f_int0", BIGINT),
                    new Variable("twolevelrow0#f_int0", BIGINT));
            projectionResult = metadata.applyProjection(session, tableHandle, inputProjections, inputAssignments);
            assertProjectionResult(projectionResult, false, expectedProjections, expectedAssignments);

            // Test reuse of virtual column handles
            // Round-1: input projections [symbol_2, symbol_2.int0]. virtual handle is created for symbol_2.int0.
            inputAssignments = getColumnHandlesFor(columnHandlesWithSymbols, ImmutableList.of("symbol_2"));
            inputProjections = ImmutableList.of(symbol2Field0, symbolVariableMapping.get("symbol_2"));
            projectionResult = metadata.applyProjection(session, tableHandle, inputProjections, inputAssignments);
            expectedProjections = ImmutableList.of(new Variable("onelevelrow0#f_int0", BIGINT), symbolVariableMapping.get("symbol_2"));
            expectedAssignments = ImmutableMap.of("onelevelrow0#f_int0", BIGINT, "symbol_2", oneLevelRowType);
            assertProjectionResult(projectionResult, false, expectedProjections, expectedAssignments);

            // Round-2: input projections [symbol_2.int0 and onelevelrow0#f_int0]. Virtual handle is reused.
            Assignment newlyCreatedColumn = getOnlyElement(projectionResult.get().getAssignments().stream()
                    .filter(handle -> handle.getVariable().equals("onelevelrow0#f_int0"))
                    .collect(toList()));
            inputAssignments = ImmutableMap.<String, ColumnHandle>builder()
                    .putAll(getColumnHandlesFor(columnHandlesWithSymbols, ImmutableList.of("symbol_2")))
                    .put(newlyCreatedColumn.getVariable(), newlyCreatedColumn.getColumn())
                    .buildOrThrow();
            inputProjections = ImmutableList.of(symbol2Field0, new Variable("onelevelrow0#f_int0", BIGINT));
            projectionResult = metadata.applyProjection(session, tableHandle, inputProjections, inputAssignments);
            expectedProjections = ImmutableList.of(new Variable("onelevelrow0#f_int0", BIGINT), new Variable("onelevelrow0#f_int0", BIGINT));
            expectedAssignments = ImmutableMap.of("onelevelrow0#f_int0", BIGINT);
            assertProjectionResult(projectionResult, false, expectedProjections, expectedAssignments);
        }
        finally {
            dropTable(tableName);
        }
    }

    private static Map<String, ColumnHandle> getColumnHandlesFor(Map<String, ColumnHandle> columnHandles, List<String> symbols)
    {
        return columnHandles.entrySet().stream()
                .filter(e -> symbols.contains(e.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static void assertProjectionResult(Optional<ProjectionApplicationResult<ConnectorTableHandle>> projectionResult, boolean shouldBeEmpty, List<ConnectorExpression> expectedProjections, Map<String, Type> expectedAssignments)
    {
        if (shouldBeEmpty) {
            assertTrue(projectionResult.isEmpty(), "expected projectionResult to be empty");
            return;
        }

        assertTrue(projectionResult.isPresent(), "expected non-empty projection result");

        ProjectionApplicationResult<ConnectorTableHandle> result = projectionResult.get();

        // Verify projections
        assertEquals(expectedProjections, result.getProjections());

        // Verify assignments
        List<Assignment> assignments = result.getAssignments();
        Map<String, Assignment> actualAssignments = uniqueIndex(assignments, Assignment::getVariable);

        for (String variable : expectedAssignments.keySet()) {
            Type expectedType = expectedAssignments.get(variable);
            assertTrue(actualAssignments.containsKey(variable));
            assertEquals(actualAssignments.get(variable).getType(), expectedType);
            assertEquals(((HiveColumnHandle) actualAssignments.get(variable).getColumn()).getType(), expectedType);
        }

        assertEquals(actualAssignments.size(), expectedAssignments.size());
        assertEquals(
                actualAssignments.values().stream().map(Assignment::getColumn).collect(toImmutableSet()),
                ((HiveTableHandle) result.getHandle()).getProjectedColumns());
    }

    @Test
    public void testApplyRedirection()
            throws Exception
    {
        SchemaTableName sourceTableName = temporaryTable("apply_redirection_tester");
        doCreateEmptyTable(sourceTableName, ORC, CREATE_TABLE_COLUMNS);
        SchemaTableName tableName = temporaryTable("apply_no_redirection_tester");
        doCreateEmptyTable(tableName, ORC, CREATE_TABLE_COLUMNS);
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            assertThat(metadata.applyTableScanRedirect(session, getTableHandle(metadata, tableName))).isEmpty();
            Optional<TableScanRedirectApplicationResult> result = metadata.applyTableScanRedirect(session, getTableHandle(metadata, sourceTableName));
            assertThat(result).isPresent();
            assertThat(result.get().getDestinationTable())
                    .isEqualTo(new CatalogSchemaTableName("hive", database, "mock_redirection_target"));
        }
        finally {
            dropTable(sourceTableName);
            dropTable(tableName);
        }
    }

    @Test
    public void testMaterializedViewMetadata()
            throws Exception
    {
        SchemaTableName sourceTableName = temporaryTable("materialized_view_tester");
        doCreateEmptyTable(sourceTableName, ORC, CREATE_TABLE_COLUMNS);
        SchemaTableName tableName = temporaryTable("mock_table");
        doCreateEmptyTable(tableName, ORC, CREATE_TABLE_COLUMNS);
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            assertThat(metadata.getMaterializedView(session, tableName)).isEmpty();
            Optional<ConnectorMaterializedViewDefinition> result = metadata.getMaterializedView(session, sourceTableName);
            assertThat(result).isPresent();
            assertThat(result.get().getOriginalSql()).isEqualTo("dummy_view_sql");
        }
        finally {
            dropTable(sourceTableName);
            dropTable(tableName);
        }
    }

    private ConnectorSession sampleSize(int sampleSize)
    {
        return getHiveSession(getHiveConfig()
                .setPartitionStatisticsSampleSize(sampleSize));
    }

    private void verifyViewCreation(SchemaTableName temporaryCreateView)
    {
        // replace works for new view
        doCreateView(temporaryCreateView, true);

        // replace works for existing view
        doCreateView(temporaryCreateView, true);

        // create fails for existing view
        try {
            doCreateView(temporaryCreateView, false);
            fail("create existing should fail");
        }
        catch (ViewAlreadyExistsException e) {
            assertEquals(e.getViewName(), temporaryCreateView);
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            // drop works when view exists
            metadata.dropView(newSession(), temporaryCreateView);
            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertThat(metadata.getView(newSession(), temporaryCreateView))
                    .isEmpty();
            assertThat(metadata.getViews(newSession(), Optional.of(temporaryCreateView.getSchemaName())))
                    .doesNotContainKey(temporaryCreateView);
            assertThat(metadata.listViews(newSession(), Optional.of(temporaryCreateView.getSchemaName())))
                    .doesNotContain(temporaryCreateView);
        }

        // drop fails when view does not exist
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.dropView(newSession(), temporaryCreateView);
            fail("drop non-existing should fail");
        }
        catch (ViewNotFoundException e) {
            assertEquals(e.getViewName(), temporaryCreateView);
        }

        // create works for new view
        doCreateView(temporaryCreateView, false);
    }

    private void doCreateView(SchemaTableName viewName, boolean replace)
    {
        String viewData = "test data";
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                viewData,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId())),
                Optional.empty(),
                Optional.empty(),
                true);

        try (Transaction transaction = newTransaction()) {
            transaction.getMetadata().createView(newSession(), viewName, definition, replace);
            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();

            assertThat(metadata.getView(newSession(), viewName))
                    .map(ConnectorViewDefinition::getOriginalSql)
                    .contains(viewData);

            Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(newSession(), Optional.of(viewName.getSchemaName()));
            assertEquals(views.size(), 1);
            assertEquals(views.get(viewName).getOriginalSql(), definition.getOriginalSql());

            assertTrue(metadata.listViews(newSession(), Optional.of(viewName.getSchemaName())).contains(viewName));
        }
    }

    protected void doCreateTable(SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        String queryId;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            queryId = session.getQueryId();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, CREATE_TABLE_COLUMNS, createTableProperties(storageFormat));

            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);

            // write the data
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // verify all new files start with the unique prefix
            HdfsContext context = new HdfsContext(session);
            for (String filePath : listAllDataFiles(context, getStagingPathRoot(outputHandle))) {
                assertThat(new Path(filePath).getName()).startsWith(session.getQueryId());
            }

            // commit the table
            metadata.finishCreateTable(session, outputHandle, fragments, ImmutableList.of());

            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            // load the new table
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), CREATE_TABLE_COLUMNS);

            // verify the data
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_DATA.getMaterializedRows());

            // verify the node version and query ID in table
            Table table = getMetastoreClient().getTable(tableName.getSchemaName(), tableName.getTableName()).get();
            assertEquals(table.getParameters().get(PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
            assertEquals(table.getParameters().get(PRESTO_QUERY_ID_NAME), queryId);

            // verify basic statistics
            HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(statistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount());
            assertEquals(statistics.getFileCount().getAsLong(), 1L);
            assertGreaterThan(statistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
            assertGreaterThan(statistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
        }
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat, List<ColumnMetadata> createTableColumns)
            throws Exception
    {
        List<String> partitionedBy = createTableColumns.stream()
                .map(ColumnMetadata::getName)
                .filter(PARTITION_COLUMN_FILTER)
                .collect(toList());

        doCreateEmptyTable(tableName, storageFormat, createTableColumns, partitionedBy);
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat, List<ColumnMetadata> createTableColumns, List<String> partitionedBy)
            throws Exception
    {
        String queryId;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            queryId = session.getQueryId();

            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, createTableColumns, createTableProperties(storageFormat, partitionedBy));
            metadata.createTable(session, tableMetadata, false);
            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);

            // load the new table
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));

            List<ColumnMetadata> expectedColumns = createTableColumns.stream()
                    .map(column -> ColumnMetadata.builder()
                            .setName(column.getName())
                            .setType(column.getType())
                            .setComment(Optional.ofNullable(column.getComment()))
                            .setExtraInfo(Optional.ofNullable(columnExtraInfo(partitionedBy.contains(column.getName()))))
                            .build())
                    .collect(toList());
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), expectedColumns);

            // verify table format
            Table table = transaction.getMetastore().getTable(tableName.getSchemaName(), tableName.getTableName()).get();
            assertEquals(table.getStorage().getStorageFormat().getInputFormat(), storageFormat.getInputFormat());

            // verify the node version and query ID
            assertEquals(table.getParameters().get(PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
            assertEquals(table.getParameters().get(PRESTO_QUERY_ID_NAME), queryId);

            // verify the table is empty
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEquals(result.getRowCount(), 0);

            // verify basic statistics
            if (partitionedBy.isEmpty()) {
                HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
                assertEquals(statistics.getRowCount().getAsLong(), 0L);
                assertEquals(statistics.getFileCount().getAsLong(), 0L);
                assertEquals(statistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertEquals(statistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
            }
        }
    }

    private void doInsert(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            insertData(tableName, CREATE_TABLE_DATA);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);

                // load the new table
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                // verify the metadata
                ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
                assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), CREATE_TABLE_COLUMNS);

                // verify the data
                resultBuilder.rows(CREATE_TABLE_DATA.getMaterializedRows());
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
                assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

                // statistics
                HiveBasicStatistics tableStatistics = getBasicStatisticsForTable(transaction, tableName);
                assertEquals(tableStatistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * (i + 1));
                assertEquals(tableStatistics.getFileCount().getAsLong(), i + 1L);
                assertGreaterThan(tableStatistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertGreaterThan(tableStatistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
            }
        }

        // test rollback
        Set<String> existingFiles;
        try (Transaction transaction = newTransaction()) {
            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());
        }

        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());

            // statistics, visible from within transaction
            HiveBasicStatistics tableStatistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(tableStatistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * 5L);

            try (Transaction otherTransaction = newTransaction()) {
                // statistics, not visible from outside transaction
                HiveBasicStatistics otherTableStatistics = getBasicStatisticsForTable(otherTransaction, tableName);
                assertEquals(otherTableStatistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * 3L);
            }

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify all temp files start with the unique prefix
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            HdfsContext context = new HdfsContext(session);
            Set<String> tempFiles = listAllDataFiles(context, stagingPathRoot);
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertThat(new Path(filePath).getName()).startsWith(session.getQueryId());
            }

            // rollback insert
            transaction.rollback();
        }

        // verify temp directory is empty
        HdfsContext context = new HdfsContext(newSession());
        assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());

        // verify the data is unchanged
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);
        }

        // verify statistics unchanged
        try (Transaction transaction = newTransaction()) {
            HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(statistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * 3L);
            assertEquals(statistics.getFileCount().getAsLong(), 3L);
        }
    }

    private void doInsertOverwriteUnpartitioned(SchemaTableName tableName)
            throws Exception
    {
        // create table with data
        doCreateEmptyTable(tableName, ORC, CREATE_TABLE_COLUMNS);
        insertData(tableName, CREATE_TABLE_DATA);

        // overwrite table with new data
        MaterializedResult.Builder overwriteDataBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_DATA.getTypes());
        MaterializedResult overwriteData = null;

        Map<String, Object> overwriteProperties = ImmutableMap.of("insert_existing_partitions_behavior", "OVERWRITE");

        for (int i = 0; i < 3; i++) {
            overwriteDataBuilder.rows(reverse(CREATE_TABLE_DATA.getMaterializedRows()));
            overwriteData = overwriteDataBuilder.build();

            insertData(tableName, overwriteData, overwriteProperties);

            // verify overwrite
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);

                // load the new table
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                // verify the metadata
                ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
                assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), CREATE_TABLE_COLUMNS);

                // verify the data
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
                assertEqualsIgnoreOrder(result.getMaterializedRows(), overwriteData.getMaterializedRows());

                // statistics
                HiveBasicStatistics tableStatistics = getBasicStatisticsForTable(transaction, tableName);
                assertEquals(tableStatistics.getRowCount().getAsLong(), overwriteData.getRowCount());
                assertEquals(tableStatistics.getFileCount().getAsLong(), 1L);
                assertGreaterThan(tableStatistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertGreaterThan(tableStatistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
            }
        }

        // test rollback
        Set<String> existingFiles;
        try (Transaction transaction = newTransaction()) {
            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());
        }

        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession(overwriteProperties);
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            for (int i = 0; i < 4; i++) {
                sink.appendPage(overwriteData.toPage());
            }
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());

            // statistics, visible from within transaction
            HiveBasicStatistics tableStatistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(tableStatistics.getRowCount().getAsLong(), overwriteData.getRowCount() * 4L);

            try (Transaction otherTransaction = newTransaction()) {
                // statistics, not visible from outside transaction
                HiveBasicStatistics otherTableStatistics = getBasicStatisticsForTable(otherTransaction, tableName);
                assertEquals(otherTableStatistics.getRowCount().getAsLong(), overwriteData.getRowCount());
            }

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify all temp files start with the unique prefix
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            HdfsContext context = new HdfsContext(session);
            Set<String> tempFiles = listAllDataFiles(context, stagingPathRoot);
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertThat(new Path(filePath).getName()).startsWith(session.getQueryId());
            }

            // rollback insert
            transaction.rollback();
        }

        // verify temp directory is empty
        HdfsContext context = new HdfsContext(newSession());
        assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());

        // verify the data is unchanged
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), overwriteData.getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);
        }

        // verify statistics unchanged
        try (Transaction transaction = newTransaction()) {
            HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(statistics.getRowCount().getAsLong(), overwriteData.getRowCount());
            assertEquals(statistics.getFileCount().getAsLong(), 1L);
        }
    }

    // These are protected so extensions to the hive connector can replace the handle classes
    protected Path getStagingPathRoot(ConnectorInsertTableHandle insertTableHandle)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertTableHandle;
        WriteInfo writeInfo = getLocationService().getQueryWriteInfo(handle.getLocationHandle());
        if (writeInfo.getWriteMode() != STAGE_AND_MOVE_TO_TARGET_DIRECTORY) {
            throw new AssertionError("writeMode is not STAGE_AND_MOVE_TO_TARGET_DIRECTORY");
        }
        return writeInfo.getWritePath();
    }

    protected Path getStagingPathRoot(ConnectorOutputTableHandle outputTableHandle)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) outputTableHandle;
        return getLocationService()
                .getQueryWriteInfo(handle.getLocationHandle())
                .getWritePath();
    }

    protected Path getTargetPathRoot(ConnectorInsertTableHandle insertTableHandle)
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle;

        return getLocationService()
                .getQueryWriteInfo(hiveInsertTableHandle.getLocationHandle())
                .getTargetPath();
    }

    protected Set<String> listAllDataFiles(Transaction transaction, String schemaName, String tableName)
            throws IOException
    {
        HdfsContext hdfsContext = new HdfsContext(newSession());
        Set<String> existingFiles = new HashSet<>();
        for (String location : listAllDataPaths(transaction.getMetastore(), schemaName, tableName)) {
            existingFiles.addAll(listAllDataFiles(hdfsContext, new Path(location)));
        }
        return existingFiles;
    }

    public static List<String> listAllDataPaths(SemiTransactionalHiveMetastore metastore, String schemaName, String tableName)
    {
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        Table table = metastore.getTable(schemaName, tableName).get();
        if (table.getStorage().getLocation() != null) {
            // For partitioned table, there should be nothing directly under this directory.
            // But including this location in the set makes the directory content assert more
            // extensive, which is desirable.
            locations.add(table.getStorage().getLocation());
        }

        Optional<List<String>> partitionNames = metastore.getPartitionNames(schemaName, tableName);
        if (partitionNames.isPresent()) {
            metastore.getPartitionsByNames(schemaName, tableName, partitionNames.get()).values().stream()
                    .map(Optional::get)
                    .map(partition -> partition.getStorage().getLocation())
                    .filter(location -> !location.startsWith(table.getStorage().getLocation()))
                    .forEach(locations::add);
        }

        return locations.build();
    }

    protected Set<String> listAllDataFiles(HdfsContext context, Path path)
            throws IOException
    {
        Set<String> result = new HashSet<>();
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
        if (fileSystem.exists(path)) {
            for (FileStatus fileStatus : fileSystem.listStatus(path)) {
                if (fileStatus.getPath().getName().startsWith(".trino")) {
                    // skip hidden files
                }
                else if (fileStatus.isFile()) {
                    result.add(fileStatus.getPath().toString());
                }
                else if (fileStatus.isDirectory()) {
                    result.addAll(listAllDataFiles(context, fileStatus.getPath()));
                }
            }
        }
        return result;
    }

    private void doInsertIntoNewPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        // insert the data
        String queryId = insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        Set<String> existingFiles;
        try (Transaction transaction = newTransaction()) {
            // verify partitions were created
            Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(tableName));
            List<String> partitionNames = transaction.getMetastore().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toImmutableList()));

            // verify the node versions in partitions
            Map<String, Optional<Partition>> partitions = getMetastoreClient().getPartitionsByNames(table, partitionNames);
            assertEquals(partitions.size(), partitionNames.size());
            for (String partitionName : partitionNames) {
                Partition partition = partitions.get(partitionName).get();
                assertEquals(partition.getParameters().get(PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
                assertEquals(partition.getParameters().get(PRESTO_QUERY_ID_NAME), queryId);
            }

            // load the new table
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

            // test rollback
            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());

            // test statistics
            for (String partitionName : partitionNames) {
                HiveBasicStatistics partitionStatistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertEquals(partitionStatistics.getRowCount().getAsLong(), 1L);
                assertEquals(partitionStatistics.getFileCount().getAsLong(), 1L);
                assertGreaterThan(partitionStatistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertGreaterThan(partitionStatistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
            }
        }

        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA_2ND.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());

            // verify all temp files start with the unique prefix
            HdfsContext context = new HdfsContext(session);
            Set<String> tempFiles = listAllDataFiles(context, getStagingPathRoot(insertTableHandle));
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertThat(new Path(filePath).getName()).startsWith(session.getQueryId());
            }

            // rollback insert
            transaction.rollback();
        }

        // verify the data is unchanged
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify temp directory is empty
            HdfsContext context = new HdfsContext(session);
            assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());
        }
    }

    private void doInsertUnsupportedWriteType(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        List<Column> columns = ImmutableList.of(new Column("dummy", HiveType.valueOf("uniontype<smallint,tinyint>"), Optional.empty()));
        List<Column> partitionColumns = ImmutableList.of(new Column("name", HIVE_STRING, Optional.empty()));

        createEmptyTable(tableName, storageFormat, columns, partitionColumns);

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            fail("expected failure");
        }
        catch (TrinoException e) {
            assertThat(e).hasMessageMatching("Inserting into Hive table .* with column type uniontype<smallint,tinyint> not supported");
        }
    }

    private void doInsertIntoExistingPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            // insert the data
            insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.beginQuery(session);
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

                // verify partitions were created
                List<String> partitionNames = transaction.getMetastore().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                        .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
                assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                        .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                        .collect(toImmutableList()));

                // load the new table
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                // verify the data
                resultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
                assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

                // test statistics
                for (String partitionName : partitionNames) {
                    HiveBasicStatistics statistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                    assertEquals(statistics.getRowCount().getAsLong(), i + 1L);
                    assertEquals(statistics.getFileCount().getAsLong(), i + 1L);
                    assertGreaterThan(statistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                    assertGreaterThan(statistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
                }
            }
        }

        // test rollback
        Set<String> existingFiles;
        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage());
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());

            // verify all temp files start with the unique prefix
            HdfsContext context = new HdfsContext(session);
            Set<String> tempFiles = listAllDataFiles(context, getStagingPathRoot(insertTableHandle));
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertThat(new Path(filePath).getName()).startsWith(session.getQueryId());
            }

            // verify statistics are visible from within of the current transaction
            List<String> partitionNames = transaction.getMetastore().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            for (String partitionName : partitionNames) {
                HiveBasicStatistics partitionStatistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertEquals(partitionStatistics.getRowCount().getAsLong(), 5L);
            }

            // rollback insert
            transaction.rollback();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data is unchanged
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify temp directory is empty
            HdfsContext hdfsContext = new HdfsContext(session);
            assertTrue(listAllDataFiles(hdfsContext, stagingPathRoot).isEmpty());

            // verify statistics have been rolled back
            List<String> partitionNames = transaction.getMetastore().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            for (String partitionName : partitionNames) {
                HiveBasicStatistics partitionStatistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertEquals(partitionStatistics.getRowCount().getAsLong(), 3L);
            }
        }
    }

    private void doInsertIntoExistingPartitionEmptyStatistics(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
        insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        eraseStatistics(tableName);

        insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        try (Transaction transaction = newTransaction()) {
            List<String> partitionNames = transaction.getMetastore().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));

            for (String partitionName : partitionNames) {
                HiveBasicStatistics statistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertThat(statistics.getRowCount()).isNotPresent();
                assertThat(statistics.getInMemoryDataSizeInBytes()).isNotPresent();
                // fileCount and rawSize statistics are computed on the fly by the metastore, thus cannot be erased
            }
        }
    }

    private static HiveBasicStatistics getBasicStatisticsForTable(Transaction transaction, SchemaTableName table)
    {
        return transaction
                .getMetastore()
                .getTableStatistics(table.getSchemaName(), table.getTableName())
                .getBasicStatistics();
    }

    private static HiveBasicStatistics getBasicStatisticsForPartition(Transaction transaction, SchemaTableName table, String partitionName)
    {
        return transaction
                .getMetastore()
                .getPartitionStatistics(table.getSchemaName(), table.getTableName(), ImmutableSet.of(partitionName))
                .get(partitionName)
                .getBasicStatistics();
    }

    private void eraseStatistics(SchemaTableName schemaTableName)
    {
        HiveMetastore metastoreClient = getMetastoreClient();
        metastoreClient.updateTableStatistics(schemaTableName.getSchemaName(), schemaTableName.getTableName(), NO_ACID_TRANSACTION, statistics -> new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
        Table table = metastoreClient.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        List<String> partitionColumns = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!table.getPartitionColumns().isEmpty()) {
            List<String> partitionNames = metastoreClient.getPartitionNamesByFilter(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionColumns, TupleDomain.all())
                    .orElse(ImmutableList.of());
            List<Partition> partitions = metastoreClient
                    .getPartitionsByNames(table, partitionNames)
                    .entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            for (Partition partition : partitions) {
                metastoreClient.updatePartitionStatistics(
                        table,
                        makePartName(partitionColumns, partition.getValues()),
                        statistics -> new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
            }
        }
    }

    /**
     * @return query id
     */
    private String insertData(SchemaTableName tableName, MaterializedResult data)
            throws Exception
    {
        return insertData(tableName, data, ImmutableMap.of());
    }

    private String insertData(SchemaTableName tableName, MaterializedResult data, Map<String, Object> sessionProperties)
            throws Exception
    {
        Path writePath;
        Path targetPath;
        String queryId;
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession(sessionProperties);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            queryId = session.getQueryId();
            writePath = getStagingPathRoot(insertTableHandle);
            targetPath = getTargetPathRoot(insertTableHandle);

            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);

            // write data
            sink.appendPage(data.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // commit the insert
            metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());
            transaction.commit();
        }

        // check that temporary files are removed
        if (!writePath.equals(targetPath)) {
            HdfsContext context = new HdfsContext(newSession());
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, writePath);
            assertFalse(fileSystem.exists(writePath));
        }

        return queryId;
    }

    private void doTestMetadataDelete(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        MaterializedResult.Builder expectedResultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        expectedResultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);

            // verify partitions were created
            List<String> partitionNames = transaction.getMetastore().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toImmutableList()));

            // verify table directory is not empty
            Set<String> filesAfterInsert = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(filesAfterInsert.isEmpty());

            // verify the data
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedResultBuilder.build().getMaterializedRows());
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            // get ds column handle
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("ds");

            // delete ds=2015-07-03
            session = newSession();
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2015-07-03"))));
            Constraint constraint = new Constraint(tupleDomain, tupleDomain.asPredicate(), tupleDomain.getDomains().orElseThrow().keySet());
            tableHandle = applyFilter(metadata, tableHandle, constraint);
            tableHandle = metadata.applyDelete(session, tableHandle).get();
            metadata.executeDelete(session, tableHandle);

            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("ds");
            int dsColumnOrdinalPosition = columnHandles.indexOf(dsColumnHandle);

            // verify the data
            ImmutableList<MaterializedRow> expectedRows = expectedResultBuilder.build().getMaterializedRows().stream()
                    .filter(row -> !"2015-07-03".equals(row.getField(dsColumnOrdinalPosition)))
                    .collect(toImmutableList());
            MaterializedResult actualAfterDelete = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(actualAfterDelete.getMaterializedRows(), expectedRows);
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("ds");

            // delete ds=2015-07-01 and 2015-07-02
            session = newSession();
            TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                    ImmutableMap.of(dsColumnHandle, Domain.create(ValueSet.ofRanges(Range.range(createUnboundedVarcharType(), utf8Slice("2015-07-01"), true, utf8Slice("2015-07-02"), true)), false)));
            Constraint constraint2 = new Constraint(tupleDomain2, tupleDomain2.asPredicate(), tupleDomain2.getDomains().orElseThrow().keySet());
            tableHandle = applyFilter(metadata, tableHandle, constraint2);
            tableHandle = metadata.applyDelete(session, tableHandle).get();
            metadata.executeDelete(session, tableHandle);

            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            session = newSession();
            MaterializedResult actualAfterDelete2 = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(actualAfterDelete2.getMaterializedRows(), ImmutableList.of());

            // verify table directory is empty
            Set<String> filesAfterDelete = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertTrue(filesAfterDelete.isEmpty());
        }
    }

    protected void assertGetRecords(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);

            ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, tableName));
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            HiveSplit hiveSplit = getHiveSplit(tableHandle, transaction, session);

            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, tableHandle, columnHandles, DynamicFilter.EMPTY);
            assertGetRecords(hiveStorageFormat, tableMetadata, hiveSplit, pageSource, columnHandles);
        }
    }

    protected HiveSplit getHiveSplit(ConnectorTableHandle tableHandle, Transaction transaction, ConnectorSession session)
    {
        List<ConnectorSplit> splits = getAllSplits(tableHandle, transaction, session);
        assertEquals(splits.size(), 1);
        return (HiveSplit) getOnlyElement(splits);
    }

    protected void assertGetRecords(
            HiveStorageFormat hiveStorageFormat,
            ConnectorTableMetadata tableMetadata,
            HiveSplit hiveSplit,
            ConnectorPageSource pageSource,
            List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        try {
            MaterializedResult result = materializeSourceDataStream(newSession(), pageSource, getTypes(columnHandles));

            assertPageSourceType(pageSource, hiveStorageFormat);

            ImmutableMap<String, Integer> columnIndex = indexColumns(tableMetadata);

            long rowNumber = 0;
            long completedBytes = 0;
            for (MaterializedRow row : result) {
                try {
                    assertValueTypes(row, tableMetadata.getColumns());
                }
                catch (RuntimeException e) {
                    throw new RuntimeException("row " + rowNumber, e);
                }

                rowNumber++;
                Integer index;
                Object value;

                // STRING
                index = columnIndex.get("t_string");
                value = row.getField(index);
                if (rowNumber % 19 == 0) {
                    assertNull(value);
                }
                else if (rowNumber % 19 == 1) {
                    assertEquals(value, "");
                }
                else {
                    assertEquals(value, "test");
                }

                // NUMBERS
                assertEquals(row.getField(columnIndex.get("t_tinyint")), (byte) (1 + rowNumber));
                assertEquals(row.getField(columnIndex.get("t_smallint")), (short) (2 + rowNumber));
                assertEquals(row.getField(columnIndex.get("t_int")), (int) (3 + rowNumber));

                index = columnIndex.get("t_bigint");
                if ((rowNumber % 13) == 0) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), 4 + rowNumber);
                }

                assertEquals((Float) row.getField(columnIndex.get("t_float")), 5.1f + rowNumber, 0.001);
                assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);

                // BOOLEAN
                index = columnIndex.get("t_boolean");
                if ((rowNumber % 3) == 2) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), (rowNumber % 3) != 0);
                }

                // TIMESTAMP
                index = columnIndex.get("t_timestamp");
                if (index != null) {
                    if ((rowNumber % 17) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlTimestamp expected = sqlTimestampOf(3, 2011, 5, 6, 7, 8, 9, 123);
                        assertEquals(row.getField(index), expected);
                    }
                }

                // BINARY
                index = columnIndex.get("t_binary");
                if (index != null) {
                    if ((rowNumber % 23) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), new SqlVarbinary("test binary".getBytes(UTF_8)));
                    }
                }

                // DATE
                index = columnIndex.get("t_date");
                if (index != null) {
                    if ((rowNumber % 37) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlDate expected = new SqlDate(toIntExact(MILLISECONDS.toDays(new DateTime(2013, 8, 9, 0, 0, 0, UTC).getMillis())));
                        assertEquals(row.getField(index), expected);
                    }
                }

                // VARCHAR(50)
                index = columnIndex.get("t_varchar");
                if (index != null) {
                    value = row.getField(index);
                    if (rowNumber % 39 == 0) {
                        assertNull(value);
                    }
                    else if (rowNumber % 39 == 1) {
                        // https://issues.apache.org/jira/browse/HIVE-13289
                        // RCBINARY reads empty VARCHAR as null
                        if (hiveStorageFormat == RCBINARY) {
                            assertNull(value);
                        }
                        else {
                            assertEquals(value, "");
                        }
                    }
                    else {
                        assertEquals(value, "test varchar");
                    }
                }

                //CHAR(25)
                index = columnIndex.get("t_char");
                if (index != null) {
                    value = row.getField(index);
                    if ((rowNumber % 41) == 0) {
                        assertNull(value);
                    }
                    else {
                        assertEquals(value, (rowNumber % 41) == 1 ? "                         " : "test char                ");
                    }
                }

                // MAP<STRING, STRING>
                index = columnIndex.get("t_map");
                if (index != null) {
                    if ((rowNumber % 27) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), ImmutableMap.of("test key", "test value"));
                    }
                }

                // ARRAY<STRING>
                index = columnIndex.get("t_array_string");
                if (index != null) {
                    if ((rowNumber % 29) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), ImmutableList.of("abc", "xyz", "data"));
                    }
                }

                // ARRAY<TIMESTAMP>
                index = columnIndex.get("t_array_timestamp");
                if (index != null) {
                    if ((rowNumber % 43) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlTimestamp expected = sqlTimestampOf(3, LocalDateTime.of(2011, 5, 6, 7, 8, 9, 123_000_000));
                        assertEquals(row.getField(index), ImmutableList.of(expected));
                    }
                }

                // ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>
                index = columnIndex.get("t_array_struct");
                if (index != null) {
                    if ((rowNumber % 31) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableList.of(expected1, expected2));
                    }
                }

                // STRUCT<s_string: STRING, s_double:DOUBLE>
                index = columnIndex.get("t_struct");
                if (index != null) {
                    if ((rowNumber % 31) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertTrue(row.getField(index) instanceof List);
                        List<?> values = (List<?>) row.getField(index);
                        assertEquals(values.size(), 2);
                        assertEquals(values.get(0), "test abc");
                        assertEquals(values.get(1), 0.1);
                    }
                }

                // MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
                index = columnIndex.get("t_complex");
                if (index != null) {
                    if ((rowNumber % 33) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableMap.of(1, ImmutableList.of(expected1, expected2)));
                    }
                }

                // NEW COLUMN
                assertNull(row.getField(columnIndex.get("new_column")));

                long newCompletedBytes = pageSource.getCompletedBytes();
                assertTrue(newCompletedBytes >= completedBytes);
                // some formats (e.g., parquet) over read the data by a bit
                assertLessThanOrEqual(newCompletedBytes, hiveSplit.getLength() + (100 * 1024));
                completedBytes = newCompletedBytes;
            }

            assertLessThanOrEqual(completedBytes, hiveSplit.getLength() + (100 * 1024));
            assertEquals(rowNumber, 100);
        }
        finally {
            pageSource.close();
        }
    }

    protected void dropTable(SchemaTableName table)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle handle = metadata.getTableHandle(session, table);
            if (handle == null) {
                return;
            }

            metadata.dropTable(session, handle);
            try {
                // todo I have no idea why this is needed... maybe there is a propagation delay in the metastore?
                metadata.dropTable(session, handle);
                fail("expected NotFoundException");
            }
            catch (TableNotFoundException expected) {
            }

            transaction.commit();
        }
    }

    protected ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(newSession(), tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private ConnectorTableHandle applyFilter(ConnectorMetadata metadata, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        return metadata.applyFilter(newSession(), tableHandle, constraint)
                .map(ConstraintApplicationResult::getHandle)
                .orElseThrow(AssertionError::new);
    }

    protected MaterializedResult readTable(
            Transaction transaction,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            ConnectorSession session,
            TupleDomain<ColumnHandle> tupleDomain,
            OptionalInt expectedSplitCount,
            Optional<HiveStorageFormat> expectedStorageFormat)
            throws Exception
    {
        tableHandle = applyFilter(transaction.getMetadata(), tableHandle, new Constraint(tupleDomain));
        List<ConnectorSplit> splits = getAllSplits(getSplits(splitManager, transaction, session, tableHandle));
        if (expectedSplitCount.isPresent()) {
            assertEquals(splits.size(), expectedSplitCount.getAsInt());
        }

        ImmutableList.Builder<MaterializedRow> allRows = ImmutableList.builder();
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, tableHandle, columnHandles, DynamicFilter.EMPTY)) {
                expectedStorageFormat.ifPresent(format -> assertPageSourceType(pageSource, format));
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                allRows.addAll(result.getMaterializedRows());
            }
        }
        return new MaterializedResult(allRows.build(), getTypes(columnHandles));
    }

    protected HiveMetastore getMetastoreClient()
    {
        return metastoreClient;
    }

    protected LocationService getLocationService()
    {
        return locationService;
    }

    protected static int getSplitCount(ConnectorSplitSource splitSource)
    {
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            splitCount += getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits().size();
        }
        return splitCount;
    }

    private List<ConnectorSplit> getAllSplits(ConnectorTableHandle tableHandle, Transaction transaction, ConnectorSession session)
    {
        return getAllSplits(getSplits(splitManager, transaction, session, tableHandle));
    }

    protected static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());
        }
        return splits.build();
    }

    protected static ConnectorSplitSource getSplits(ConnectorSplitManager splitManager, Transaction transaction, ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return splitManager.getSplits(transaction.getTransactionHandle(), session, tableHandle, UNGROUPED_SCHEDULING, DynamicFilter.EMPTY, Constraint.alwaysTrue());
    }

    protected String getPartitionId(Object partition)
    {
        return ((HivePartition) partition).getPartitionId();
    }

    protected static void assertPageSourceType(ConnectorPageSource pageSource, HiveStorageFormat hiveStorageFormat)
    {
        if (pageSource instanceof RecordPageSource) {
            RecordCursor hiveRecordCursor = ((RecordPageSource) pageSource).getCursor();
            hiveRecordCursor = ((HiveRecordCursor) hiveRecordCursor).getRegularColumnRecordCursor();
            if (hiveRecordCursor instanceof HiveBucketValidationRecordCursor) {
                hiveRecordCursor = ((HiveBucketValidationRecordCursor) hiveRecordCursor).delegate();
            }
            if (hiveRecordCursor instanceof HiveCoercionRecordCursor) {
                hiveRecordCursor = ((HiveCoercionRecordCursor) hiveRecordCursor).getRegularColumnRecordCursor();
            }
            assertInstanceOf(hiveRecordCursor, recordCursorType(), hiveStorageFormat.name());
        }
        else {
            assertInstanceOf(((HivePageSource) pageSource).getPageSource(), pageSourceType(hiveStorageFormat), hiveStorageFormat.name());
        }
    }

    private static Class<? extends RecordCursor> recordCursorType()
    {
        return GenericHiveRecordCursor.class;
    }

    private static Class<? extends ConnectorPageSource> pageSourceType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case RCTEXT:
            case RCBINARY:
                return RcFilePageSource.class;
            case ORC:
                return OrcPageSource.class;
            case PARQUET:
                return ParquetPageSource.class;
            default:
                throw new AssertionError("File type does not use a PageSource: " + hiveStorageFormat);
        }
    }

    private static void assertValueTypes(MaterializedRow row, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            Object value = row.getField(columnIndex);
            if (value != null) {
                if (BOOLEAN.equals(column.getType())) {
                    assertInstanceOf(value, Boolean.class);
                }
                else if (TINYINT.equals(column.getType())) {
                    assertInstanceOf(value, Byte.class);
                }
                else if (SMALLINT.equals(column.getType())) {
                    assertInstanceOf(value, Short.class);
                }
                else if (INTEGER.equals(column.getType())) {
                    assertInstanceOf(value, Integer.class);
                }
                else if (BIGINT.equals(column.getType())) {
                    assertInstanceOf(value, Long.class);
                }
                else if (DOUBLE.equals(column.getType())) {
                    assertInstanceOf(value, Double.class);
                }
                else if (REAL.equals(column.getType())) {
                    assertInstanceOf(value, Float.class);
                }
                else if (column.getType() instanceof VarcharType) {
                    assertInstanceOf(value, String.class);
                }
                else if (column.getType() instanceof CharType) {
                    assertInstanceOf(value, String.class);
                }
                else if (VARBINARY.equals(column.getType())) {
                    assertInstanceOf(value, SqlVarbinary.class);
                }
                else if (TIMESTAMP_MILLIS.equals(column.getType())) {
                    assertInstanceOf(value, SqlTimestamp.class);
                }
                else if (TIMESTAMP_WITH_TIME_ZONE.equals(column.getType())) {
                    assertInstanceOf(value, SqlTimestampWithTimeZone.class);
                }
                else if (DATE.equals(column.getType())) {
                    assertInstanceOf(value, SqlDate.class);
                }
                else if (column.getType() instanceof ArrayType || column.getType() instanceof RowType) {
                    assertInstanceOf(value, List.class);
                }
                else if (column.getType() instanceof MapType) {
                    assertInstanceOf(value, Map.class);
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, ColumnMetadata> map, String name, Type type, boolean partitionKey)
    {
        assertTrue(map.containsKey(name));
        ColumnMetadata column = map.get(name);
        assertEquals(column.getType(), type, name);
        assertEquals(column.getExtraInfo(), columnExtraInfo(partitionKey));
    }

    protected static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.buildOrThrow();
    }

    protected static ImmutableMap<String, Integer> indexColumns(ConnectorTableMetadata tableMetadata)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            index.put(columnMetadata.getName(), i);
            i++;
        }
        return index.buildOrThrow();
    }

    protected SchemaTableName temporaryTable(String tableName)
    {
        return temporaryTable(database, tableName);
    }

    protected static SchemaTableName temporaryTable(String database, String tableName)
    {
        String randomName = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return new SchemaTableName(database, TEMPORARY_TABLE_PREFIX + tableName + "_" + randomName);
    }

    protected static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat)
    {
        return createTableProperties(storageFormat, ImmutableList.of());
    }

    protected static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat, Iterable<String> parititonedBy)
    {
        return ImmutableMap.<String, Object>builder()
                .put(STORAGE_FORMAT_PROPERTY, storageFormat)
                .put(PARTITIONED_BY_PROPERTY, ImmutableList.copyOf(parititonedBy))
                .put(BUCKETED_BY_PROPERTY, ImmutableList.of())
                .put(BUCKET_COUNT_PROPERTY, 0)
                .put(SORTED_BY_PROPERTY, ImmutableList.of())
                .buildOrThrow();
    }

    protected static List<ColumnHandle> filterNonHiddenColumnHandles(Collection<ColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                .collect(toList());
    }

    protected static List<ColumnMetadata> filterNonHiddenColumnMetadata(Collection<ColumnMetadata> columnMetadatas)
    {
        return columnMetadatas.stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(toList());
    }

    private void createEmptyTable(SchemaTableName schemaTableName, HiveStorageFormat hiveStorageFormat, List<Column> columns, List<Column> partitionColumns)
            throws Exception
    {
        createEmptyTable(schemaTableName, hiveStorageFormat, columns, partitionColumns, Optional.empty(), false);
    }

    private void createEmptyTable(
            SchemaTableName schemaTableName,
            HiveStorageFormat hiveStorageFormat,
            List<Column> columns,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty)
            throws Exception
    {
        createEmptyTable(schemaTableName, hiveStorageFormat, columns, partitionColumns, bucketProperty, false);
    }

    protected void createEmptyTable(
            SchemaTableName schemaTableName,
            HiveStorageFormat hiveStorageFormat,
            List<Column> columns,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            boolean isTransactional)
            throws Exception
    {
        Path targetPath;

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();

            String tableOwner = session.getUser();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();

            LocationService locationService = getLocationService();
            LocationHandle locationHandle = locationService.forNewTable(transaction.getMetastore(), session, schemaName, tableName, Optional.empty());
            targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();

            ImmutableMap.Builder<String, String> tableParamBuilder = ImmutableMap.<String, String>builder()
                    .put(PRESTO_VERSION_NAME, TEST_SERVER_VERSION)
                    .put(PRESTO_QUERY_ID_NAME, session.getQueryId());
            if (isTransactional) {
                tableParamBuilder.put(TRANSACTIONAL, "true");
            }
            Table.Builder tableBuilder = Table.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setOwner(Optional.of(tableOwner))
                    .setTableType(TableType.MANAGED_TABLE.name())
                    .setParameters(tableParamBuilder.buildOrThrow())
                    .setDataColumns(columns)
                    .setPartitionColumns(partitionColumns);

            tableBuilder.getStorageBuilder()
                    .setLocation(targetPath.toString())
                    .setStorageFormat(StorageFormat.create(hiveStorageFormat.getSerde(), hiveStorageFormat.getInputFormat(), hiveStorageFormat.getOutputFormat()))
                    .setBucketProperty(bucketProperty)
                    .setSerdeParameters(ImmutableMap.of());

            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(tableOwner, session.getUser());
            transaction.getMetastore().createTable(session, tableBuilder.build(), principalPrivileges, Optional.empty(), Optional.empty(), true, EMPTY_TABLE_STATISTICS, false);

            transaction.commit();
        }

        HdfsContext context = new HdfsContext(newSession());
        List<String> targetDirectoryList = listDirectory(context, targetPath);
        assertEquals(targetDirectoryList, ImmutableList.of());
    }

    private void alterBucketProperty(SchemaTableName schemaTableName, Optional<HiveBucketProperty> bucketProperty)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();

            String tableOwner = session.getUser();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();

            Optional<Table> table = transaction.getMetastore().getTable(schemaName, tableName);
            Table.Builder tableBuilder = Table.builder(table.get());
            tableBuilder.getStorageBuilder().setBucketProperty(bucketProperty);
            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(tableOwner, session.getUser());
            transaction.getMetastore().replaceTable(schemaName, tableName, tableBuilder.build(), principalPrivileges);

            transaction.commit();
        }
    }

    protected PrincipalPrivileges testingPrincipalPrivilege(ConnectorSession session)
    {
        return testingPrincipalPrivilege(session.getUser(), session.getUser());
    }

    protected PrincipalPrivileges testingPrincipalPrivilege(String tableOwner, String grantor)
    {
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.SELECT, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.INSERT, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.UPDATE, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.DELETE, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .build(),
                ImmutableMultimap.of());
    }

    private List<String> listDirectory(HdfsContext context, Path path)
            throws IOException
    {
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
        return Arrays.stream(fileSystem.listStatus(path))
                .map(FileStatus::getPath)
                .map(Path::getName)
                .filter(name -> !name.startsWith(".trino"))
                .collect(toList());
    }

    @Test
    public void testTransactionDeleteInsert()
            throws Exception
    {
        doTestTransactionDeleteInsert(
                RCBINARY,
                true,
                ImmutableList.<TransactionDeleteInsertTestCase>builder()
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_RIGHT_AWAY, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_DELETE, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_BEGIN_INSERT, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_APPEND_PAGE, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_SINK_FINISH, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_FINISH_INSERT, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new AddPartitionFailure())))
                        .add(new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new DirectoryRenameFailure())))
                        .add(new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new FileRenameFailure())))
                        .add(new TransactionDeleteInsertTestCase(true, false, COMMIT, Optional.of(new DropPartitionFailure())))
                        .add(new TransactionDeleteInsertTestCase(true, true, COMMIT, Optional.empty()))
                        .build());
    }

    @Test
    public void testPreferredInsertLayout()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("empty_partitioned_table");

        try {
            Column partitioningColumn = new Column("column2", HIVE_STRING, Optional.empty());
            List<Column> columns = ImmutableList.of(
                    new Column("column1", HIVE_STRING, Optional.empty()),
                    partitioningColumn);
            createEmptyTable(tableName, ORC, columns, ImmutableList.of(partitioningColumn));

            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorSession session = newSession();
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                Optional<ConnectorTableLayout> insertLayout = metadata.getInsertLayout(session, tableHandle);
                assertTrue(insertLayout.isPresent());
                assertFalse(insertLayout.get().getPartitioning().isPresent());
                assertEquals(insertLayout.get().getPartitionColumns(), ImmutableList.of(partitioningColumn.getName()));
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertBucketedTableLayout()
            throws Exception
    {
        insertBucketedTableLayout(false);
    }

    @Test
    public void testInsertBucketedTransactionalTableLayout()
            throws Exception
    {
        insertBucketedTableLayout(true);
    }

    protected void insertBucketedTableLayout(boolean transactional)
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("empty_bucketed_table");
        try {
            List<Column> columns = ImmutableList.of(
                    new Column("column1", HIVE_STRING, Optional.empty()),
                    new Column("column2", HIVE_LONG, Optional.empty()));
            HiveBucketProperty bucketProperty = new HiveBucketProperty(ImmutableList.of("column1"), BUCKETING_V1, 4, ImmutableList.of());
            createEmptyTable(tableName, ORC, columns, ImmutableList.of(), Optional.of(bucketProperty), transactional);

            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorSession session = newSession();
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                Optional<ConnectorTableLayout> insertLayout = metadata.getInsertLayout(session, tableHandle);
                assertTrue(insertLayout.isPresent());
                ConnectorPartitioningHandle partitioningHandle = new HivePartitioningHandle(
                        bucketProperty.getBucketingVersion(),
                        bucketProperty.getBucketCount(),
                        ImmutableList.of(HIVE_STRING),
                        OptionalInt.empty(),
                        false);
                assertEquals(insertLayout.get().getPartitioning(), Optional.of(partitioningHandle));
                assertEquals(insertLayout.get().getPartitionColumns(), ImmutableList.of("column1"));
                ConnectorBucketNodeMap connectorBucketNodeMap = nodePartitioningProvider.getBucketNodeMap(transaction.getTransactionHandle(), session, partitioningHandle);
                assertEquals(connectorBucketNodeMap.getBucketCount(), 4);
                assertFalse(connectorBucketNodeMap.hasFixedMapping());
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertPartitionedBucketedTableLayout()
            throws Exception
    {
        insertPartitionedBucketedTableLayout(false);
    }

    @Test
    public void testInsertPartitionedBucketedTransactionalTableLayout()
            throws Exception
    {
        insertPartitionedBucketedTableLayout(true);
    }

    protected void insertPartitionedBucketedTableLayout(boolean transactional)
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("empty_partitioned_table");
        try {
            Column partitioningColumn = new Column("column2", HIVE_LONG, Optional.empty());
            List<Column> columns = ImmutableList.of(
                    new Column("column1", HIVE_STRING, Optional.empty()),
                    partitioningColumn);
            HiveBucketProperty bucketProperty = new HiveBucketProperty(ImmutableList.of("column1"), BUCKETING_V1, 4, ImmutableList.of());
            createEmptyTable(tableName, ORC, columns, ImmutableList.of(partitioningColumn), Optional.of(bucketProperty), transactional);

            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorSession session = newSession();
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                Optional<ConnectorTableLayout> insertLayout = metadata.getInsertLayout(session, tableHandle);
                assertTrue(insertLayout.isPresent());
                ConnectorPartitioningHandle partitioningHandle = new HivePartitioningHandle(
                        bucketProperty.getBucketingVersion(),
                        bucketProperty.getBucketCount(),
                        ImmutableList.of(HIVE_STRING),
                        OptionalInt.empty(),
                        true);
                assertEquals(insertLayout.get().getPartitioning(), Optional.of(partitioningHandle));
                assertEquals(insertLayout.get().getPartitionColumns(), ImmutableList.of("column1", "column2"));
                ConnectorBucketNodeMap connectorBucketNodeMap = nodePartitioningProvider.getBucketNodeMap(transaction.getTransactionHandle(), session, partitioningHandle);
                assertEquals(connectorBucketNodeMap.getBucketCount(), 32);
                assertTrue(connectorBucketNodeMap.hasFixedMapping());
                assertEquals(connectorBucketNodeMap.getFixedMapping().size(), 32);
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testPreferredCreateTableLayout()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            Optional<ConnectorTableLayout> newTableLayout = metadata.getNewTableLayout(
                    session,
                    new ConnectorTableMetadata(
                            new SchemaTableName("schema", "table"),
                            ImmutableList.of(
                                    new ColumnMetadata("column1", BIGINT),
                                    new ColumnMetadata("column2", BIGINT)),
                            ImmutableMap.of(
                                    PARTITIONED_BY_PROPERTY, ImmutableList.of("column2"),
                                    BUCKETED_BY_PROPERTY, ImmutableList.of(),
                                    BUCKET_COUNT_PROPERTY, 0,
                                    SORTED_BY_PROPERTY, ImmutableList.of())));
            assertTrue(newTableLayout.isPresent());
            assertFalse(newTableLayout.get().getPartitioning().isPresent());
            assertEquals(newTableLayout.get().getPartitionColumns(), ImmutableList.of("column2"));
        }
    }

    @Test
    public void testCreateBucketedTableLayout()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            Optional<ConnectorTableLayout> newTableLayout = metadata.getNewTableLayout(
                    session,
                    new ConnectorTableMetadata(
                            new SchemaTableName("schema", "table"),
                            ImmutableList.of(
                                    new ColumnMetadata("column1", BIGINT),
                                    new ColumnMetadata("column2", BIGINT)),
                            ImmutableMap.of(
                                    PARTITIONED_BY_PROPERTY, ImmutableList.of(),
                                    BUCKETED_BY_PROPERTY, ImmutableList.of("column1"),
                                    BUCKET_COUNT_PROPERTY, 10,
                                    SORTED_BY_PROPERTY, ImmutableList.of())));
            assertTrue(newTableLayout.isPresent());
            ConnectorPartitioningHandle partitioningHandle = new HivePartitioningHandle(
                    BUCKETING_V1,
                    10,
                    ImmutableList.of(HIVE_LONG),
                    OptionalInt.empty(),
                    false);
            assertEquals(newTableLayout.get().getPartitioning(), Optional.of(partitioningHandle));
            assertEquals(newTableLayout.get().getPartitionColumns(), ImmutableList.of("column1"));
            ConnectorBucketNodeMap connectorBucketNodeMap = nodePartitioningProvider.getBucketNodeMap(transaction.getTransactionHandle(), session, partitioningHandle);
            assertEquals(connectorBucketNodeMap.getBucketCount(), 10);
            assertFalse(connectorBucketNodeMap.hasFixedMapping());
        }
    }

    @Test
    public void testCreatePartitionedBucketedTableLayout()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            Optional<ConnectorTableLayout> newTableLayout = metadata.getNewTableLayout(
                    session,
                    new ConnectorTableMetadata(
                            new SchemaTableName("schema", "table"),
                            ImmutableList.of(
                                    new ColumnMetadata("column1", BIGINT),
                                    new ColumnMetadata("column2", BIGINT)),
                            ImmutableMap.of(
                                    PARTITIONED_BY_PROPERTY, ImmutableList.of("column2"),
                                    BUCKETED_BY_PROPERTY, ImmutableList.of("column1"),
                                    BUCKET_COUNT_PROPERTY, 10,
                                    SORTED_BY_PROPERTY, ImmutableList.of())));
            assertTrue(newTableLayout.isPresent());
            ConnectorPartitioningHandle partitioningHandle = new HivePartitioningHandle(
                    BUCKETING_V1,
                    10,
                    ImmutableList.of(HIVE_LONG),
                    OptionalInt.empty(),
                    true);
            assertEquals(newTableLayout.get().getPartitioning(), Optional.of(partitioningHandle));
            assertEquals(newTableLayout.get().getPartitionColumns(), ImmutableList.of("column1", "column2"));
            ConnectorBucketNodeMap connectorBucketNodeMap = nodePartitioningProvider.getBucketNodeMap(transaction.getTransactionHandle(), session, partitioningHandle);
            assertEquals(connectorBucketNodeMap.getBucketCount(), 32);
            assertTrue(connectorBucketNodeMap.hasFixedMapping());
            assertEquals(connectorBucketNodeMap.getFixedMapping().size(), 32);
        }
    }

    @Test
    public void testNewDirectoryPermissions()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("empty_file");
        List<Column> columns = ImmutableList.of(new Column("test", HIVE_STRING, Optional.empty()));
        createEmptyTable(tableName, ORC, columns, ImmutableList.of(), Optional.empty());
        try {
            Transaction transaction = newTransaction();
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);

            Table table = transaction.getMetastore()
                    .getTable(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow();

            // create new directory and set directory permission after creation
            HdfsContext context = new HdfsContext(session);
            Path location = new Path(table.getStorage().getLocation());
            Path defaultPath = new Path(location + "/defaultperms");
            createDirectory(context, hdfsEnvironment, defaultPath);
            FileStatus defaultFsStatus = hdfsEnvironment.getFileSystem(context, defaultPath).getFileStatus(defaultPath);
            assertEquals(defaultFsStatus.getPermission().toOctal(), 777);

            // use hdfs config that skips setting directory permissions after creation
            HdfsConfig configWithSkip = new HdfsConfig();
            configWithSkip.setNewDirectoryPermissions(HdfsConfig.SKIP_DIR_PERMISSIONS);
            HdfsEnvironment hdfsEnvironmentWithSkip = new HdfsEnvironment(
                    createTestHdfsConfiguration(),
                    configWithSkip,
                    new NoHdfsAuthentication());

            Path skipPath = new Path(location + "/skipperms");
            createDirectory(context, hdfsEnvironmentWithSkip, skipPath);
            FileStatus skipFsStatus = hdfsEnvironmentWithSkip.getFileSystem(context, skipPath).getFileStatus(skipPath);
            assertEquals(skipFsStatus.getPermission().toOctal(), 755);
        }
        finally {
            dropTable(tableName);
        }
    }

    protected void doTestTransactionDeleteInsert(HiveStorageFormat storageFormat, boolean allowInsertExisting, List<TransactionDeleteInsertTestCase> testCases)
            throws Exception
    {
        // There are 4 types of operations on a partition: add, drop, alter (drop then add), insert existing.
        // There are 12 partitions in this test, 3 for each type.
        // 3 is chosen to verify that cleanups, commit aborts, rollbacks are always as complete as possible regardless of failure.
        MaterializedResult beforeData =
                MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                        .row(110L, "a", "alter1")
                        .row(120L, "a", "insert1")
                        .row(140L, "a", "drop1")
                        .row(210L, "b", "drop2")
                        .row(310L, "c", "alter2")
                        .row(320L, "c", "alter3")
                        .row(510L, "e", "drop3")
                        .row(610L, "f", "insert2")
                        .row(620L, "f", "insert3")
                        .build();
        Domain domainToDrop = Domain.create(ValueSet.of(
                createUnboundedVarcharType(),
                utf8Slice("alter1"), utf8Slice("alter2"), utf8Slice("alter3"), utf8Slice("drop1"), utf8Slice("drop2"), utf8Slice("drop3")),
                false);
        List<MaterializedRow> extraRowsForInsertExisting = ImmutableList.of();
        if (allowInsertExisting) {
            extraRowsForInsertExisting = MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                    .row(121L, "a", "insert1")
                    .row(611L, "f", "insert2")
                    .row(621L, "f", "insert3")
                    .build()
                    .getMaterializedRows();
        }
        MaterializedResult insertData =
                MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                        .row(111L, "a", "alter1")
                        .row(131L, "a", "add1")
                        .row(221L, "b", "add2")
                        .row(311L, "c", "alter2")
                        .row(321L, "c", "alter3")
                        .row(411L, "d", "add3")
                        .rows(extraRowsForInsertExisting)
                        .build();
        MaterializedResult afterData =
                MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                        .row(120L, "a", "insert1")
                        .row(610L, "f", "insert2")
                        .row(620L, "f", "insert3")
                        .rows(insertData.getMaterializedRows())
                        .build();

        for (TransactionDeleteInsertTestCase testCase : testCases) {
            SchemaTableName temporaryDeleteInsert = temporaryTable("delete_insert");
            try {
                createEmptyTable(
                        temporaryDeleteInsert,
                        storageFormat,
                        ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty())),
                        ImmutableList.of(new Column("pk1", HIVE_STRING, Optional.empty()), new Column("pk2", HIVE_STRING, Optional.empty())));
                insertData(temporaryDeleteInsert, beforeData);
                try {
                    doTestTransactionDeleteInsert(
                            storageFormat,
                            temporaryDeleteInsert,
                            domainToDrop,
                            insertData,
                            testCase.isExpectCommitedData() ? afterData : beforeData,
                            testCase.getTag(),
                            testCase.isExpectQuerySucceed(),
                            testCase.getConflictTrigger());
                }
                catch (AssertionError e) {
                    throw new AssertionError(format("Test case: %s", testCase), e);
                }
            }
            finally {
                dropTable(temporaryDeleteInsert);
            }
        }
    }

    private void doTestTransactionDeleteInsert(
            HiveStorageFormat storageFormat,
            SchemaTableName tableName,
            Domain domainToDrop,
            MaterializedResult insertData,
            MaterializedResult expectedData,
            TransactionDeleteInsertTestTag tag,
            boolean expectQuerySucceed,
            Optional<ConflictTrigger> conflictTrigger)
            throws Exception
    {
        Path writePath = null;
        Path targetPath = null;

        try (Transaction transaction = newTransaction()) {
            try {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                ConnectorSession session;
                rollbackIfEquals(tag, ROLLBACK_RIGHT_AWAY);

                // Query 1: delete
                session = newSession();
                HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("pk2");
                TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumnHandle, domainToDrop));
                Constraint constraint = new Constraint(tupleDomain, tupleDomain.asPredicate(), tupleDomain.getDomains().orElseThrow().keySet());
                tableHandle = applyFilter(metadata, tableHandle, constraint);
                tableHandle = metadata.applyDelete(session, tableHandle).get();
                metadata.executeDelete(session, tableHandle);
                rollbackIfEquals(tag, ROLLBACK_AFTER_DELETE);

                // Query 2: insert
                session = newSession();
                ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
                rollbackIfEquals(tag, ROLLBACK_AFTER_BEGIN_INSERT);
                writePath = getStagingPathRoot(insertTableHandle);
                targetPath = getTargetPathRoot(insertTableHandle);
                ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
                sink.appendPage(insertData.toPage());
                rollbackIfEquals(tag, ROLLBACK_AFTER_APPEND_PAGE);
                Collection<Slice> fragments = getFutureValue(sink.finish());
                rollbackIfEquals(tag, ROLLBACK_AFTER_SINK_FINISH);
                metadata.finishInsert(session, insertTableHandle, fragments, ImmutableList.of());
                rollbackIfEquals(tag, ROLLBACK_AFTER_FINISH_INSERT);

                assertEquals(tag, COMMIT);

                if (conflictTrigger.isPresent()) {
                    JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
                    List<PartitionUpdate> partitionUpdates = fragments.stream()
                            .map(Slice::getBytes)
                            .map(partitionUpdateCodec::fromJson)
                            .collect(toList());
                    conflictTrigger.get().triggerConflict(session, tableName, insertTableHandle, partitionUpdates);
                }
                transaction.commit();
                if (conflictTrigger.isPresent()) {
                    assertTrue(expectQuerySucceed);
                    conflictTrigger.get().verifyAndCleanup(session, tableName);
                }
            }
            catch (TestingRollbackException e) {
                transaction.rollback();
            }
            catch (TrinoException e) {
                assertFalse(expectQuerySucceed);
                if (conflictTrigger.isPresent()) {
                    conflictTrigger.get().verifyAndCleanup(newSession(), tableName);
                }
            }
        }

        // check that temporary files are removed
        if (writePath != null && !writePath.equals(targetPath)) {
            HdfsContext context = new HdfsContext(newSession());
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, writePath);
            assertFalse(fileSystem.exists(writePath));
        }

        try (Transaction transaction = newTransaction()) {
            // verify partitions
            List<String> partitionNames = transaction.getMetastore()
                    .getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            assertEqualsIgnoreOrder(
                    partitionNames,
                    expectedData.getMaterializedRows().stream()
                            .map(row -> format("pk1=%s/pk2=%s", row.getField(1), row.getField(2)))
                            .distinct()
                            .collect(toImmutableList()));

            // load the new table
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.beginQuery(session);
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedData.getMaterializedRows());
        }
    }

    private static void rollbackIfEquals(TransactionDeleteInsertTestTag tag, TransactionDeleteInsertTestTag expectedTag)
    {
        if (expectedTag == tag) {
            throw new TestingRollbackException();
        }
    }

    private static class TestingRollbackException
            extends RuntimeException
    {
    }

    protected static class TransactionDeleteInsertTestCase
    {
        private final boolean expectCommitedData;
        private final boolean expectQuerySucceed;
        private final TransactionDeleteInsertTestTag tag;
        private final Optional<ConflictTrigger> conflictTrigger;

        public TransactionDeleteInsertTestCase(boolean expectCommitedData, boolean expectQuerySucceed, TransactionDeleteInsertTestTag tag, Optional<ConflictTrigger> conflictTrigger)
        {
            this.expectCommitedData = expectCommitedData;
            this.expectQuerySucceed = expectQuerySucceed;
            this.tag = tag;
            this.conflictTrigger = conflictTrigger;
        }

        public boolean isExpectCommitedData()
        {
            return expectCommitedData;
        }

        public boolean isExpectQuerySucceed()
        {
            return expectQuerySucceed;
        }

        public TransactionDeleteInsertTestTag getTag()
        {
            return tag;
        }

        public Optional<ConflictTrigger> getConflictTrigger()
        {
            return conflictTrigger;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("tag", tag)
                    .add("conflictTrigger", conflictTrigger.map(conflictTrigger -> conflictTrigger.getClass().getName()))
                    .add("expectCommitedData", expectCommitedData)
                    .add("expectQuerySucceed", expectQuerySucceed)
                    .toString();
        }
    }

    protected enum TransactionDeleteInsertTestTag
    {
        ROLLBACK_RIGHT_AWAY,
        ROLLBACK_AFTER_DELETE,
        ROLLBACK_AFTER_BEGIN_INSERT,
        ROLLBACK_AFTER_APPEND_PAGE,
        ROLLBACK_AFTER_SINK_FINISH,
        ROLLBACK_AFTER_FINISH_INSERT,
        COMMIT,
    }

    protected interface ConflictTrigger
    {
        void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
                throws IOException;

        void verifyAndCleanup(ConnectorSession session, SchemaTableName tableName)
                throws IOException;
    }

    protected class AddPartitionFailure
            implements ConflictTrigger
    {
        private final ImmutableList<String> copyPartitionFrom = ImmutableList.of("a", "insert1");
        private final String partitionNameToConflict = "pk1=b/pk2=add2";
        private Partition conflictPartition;

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
        {
            // This method bypasses transaction interface because this method is inherently hacky and doesn't work well with the transaction abstraction.
            // Additionally, this method is not part of a test. Its purpose is to set up an environment for another test.
            HiveMetastore metastoreClient = getMetastoreClient();
            Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(tableName));
            Optional<Partition> partition = metastoreClient.getPartition(table, copyPartitionFrom);
            conflictPartition = Partition.builder(partition.get())
                    .setValues(toPartitionValues(partitionNameToConflict))
                    .build();
            metastoreClient.addPartitions(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    ImmutableList.of(new PartitionWithStatistics(conflictPartition, partitionNameToConflict, PartitionStatistics.empty())));
        }

        @Override
        public void verifyAndCleanup(ConnectorSession session, SchemaTableName tableName)
        {
            // This method bypasses transaction interface because this method is inherently hacky and doesn't work well with the transaction abstraction.
            // Additionally, this method is not part of a test. Its purpose is to set up an environment for another test.
            HiveMetastore metastoreClient = getMetastoreClient();
            Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(tableName));
            Optional<Partition> actualPartition = metastoreClient.getPartition(table, toPartitionValues(partitionNameToConflict));
            // Make sure the partition inserted to trigger conflict was not overwritten
            // Checking storage location is sufficient because implement never uses .../pk1=a/pk2=a2 as the directory for partition [b, b2].
            assertEquals(actualPartition.get().getStorage().getLocation(), conflictPartition.getStorage().getLocation());
            metastoreClient.dropPartition(tableName.getSchemaName(), tableName.getTableName(), conflictPartition.getValues(), false);
        }
    }

    protected class DropPartitionFailure
            implements ConflictTrigger
    {
        private final ImmutableList<String> partitionValueToConflict = ImmutableList.of("b", "drop2");

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
        {
            // This method bypasses transaction interface because this method is inherently hacky and doesn't work well with the transaction abstraction.
            // Additionally, this method is not part of a test. Its purpose is to set up an environment for another test.
            HiveMetastore metastoreClient = getMetastoreClient();
            metastoreClient.dropPartition(tableName.getSchemaName(), tableName.getTableName(), partitionValueToConflict, false);
        }

        @Override
        public void verifyAndCleanup(ConnectorSession session, SchemaTableName tableName)
        {
            // Do not add back the deleted partition because the implementation is expected to move forward instead of backward when delete fails
        }
    }

    protected class DirectoryRenameFailure
            implements ConflictTrigger
    {
        private HdfsContext context;
        private Path path;

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
        {
            Path writePath = getStagingPathRoot(insertTableHandle);
            Path targetPath = getTargetPathRoot(insertTableHandle);
            if (writePath.equals(targetPath)) {
                // This conflict does not apply. Trigger a rollback right away so that this test case passes.
                throw new TestingRollbackException();
            }
            path = new Path(targetPath + "/pk1=b/pk2=add2");
            context = new HdfsContext(session);
            createDirectory(context, hdfsEnvironment, path);
        }

        @Override
        public void verifyAndCleanup(ConnectorSession session, SchemaTableName tableName)
                throws IOException
        {
            assertEquals(listDirectory(context, path), ImmutableList.of());
            hdfsEnvironment.getFileSystem(context, path).delete(path, false);
        }
    }

    protected class FileRenameFailure
            implements ConflictTrigger
    {
        private HdfsContext context;
        private Path path;

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
                throws IOException
        {
            for (PartitionUpdate partitionUpdate : partitionUpdates) {
                if ("pk2=insert2".equals(partitionUpdate.getTargetPath().getName())) {
                    path = new Path(partitionUpdate.getTargetPath(), partitionUpdate.getFileNames().get(0));
                    break;
                }
            }
            assertNotNull(path);

            context = new HdfsContext(session);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
            fileSystem.createNewFile(path);
        }

        @Override
        public void verifyAndCleanup(ConnectorSession session, SchemaTableName tableName)
                throws IOException
        {
            // The file we added to trigger a conflict was cleaned up because it matches the query prefix.
            // Consider this the same as a network failure that caused the successful creation of file not reported to the caller.
            assertFalse(hdfsEnvironment.getFileSystem(context, path).exists(path));
        }
    }
}
