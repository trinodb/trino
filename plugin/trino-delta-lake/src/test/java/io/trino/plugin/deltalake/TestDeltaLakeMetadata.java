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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreModule;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorContext;
import io.trino.tests.BogusType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestDeltaLakeMetadata
{
    private static final String DATABASE_NAME = "mock_database";

    private static final ColumnMetadata BIGINT_COLUMN_1 = new ColumnMetadata("bigint_column1", BIGINT);
    private static final ColumnMetadata BIGINT_COLUMN_2 = new ColumnMetadata("bigint_column2", BIGINT);
    private static final ColumnMetadata TIMESTAMP_COLUMN = new ColumnMetadata("timestamp_column", TIMESTAMP_MILLIS);
    private static final ColumnMetadata MISSING_COLUMN = new ColumnMetadata("missing_column", BIGINT);

    private static final RowType BOGUS_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("test_field"), BogusType.BOGUS)));
    private static final RowType NESTED_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("child1"), INTEGER),
            new RowType.Field(Optional.of("child2"), INTEGER)));
    private static final RowType HIGHLY_NESTED_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("grandparent"), RowType.from(ImmutableList.of(
                    new RowType.Field(Optional.of("parent"), RowType.from(ImmutableList.of(
                            new RowType.Field(Optional.of("child"), INTEGER)))))))));

    private static final DeltaLakeColumnHandle BOOLEAN_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("boolean_column_name", BooleanType.BOOLEAN, OptionalInt.empty(), "boolean_column_name", BooleanType.BOOLEAN, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle DOUBLE_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("double_column_name", DoubleType.DOUBLE, OptionalInt.empty(), "double_column_name", DoubleType.DOUBLE, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle BOGUS_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("bogus_column_name", BogusType.BOGUS, OptionalInt.empty(), "bogus_column_name", BogusType.BOGUS, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle VARCHAR_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("varchar_column_name", VarcharType.VARCHAR, OptionalInt.empty(), "varchar_column_name", VarcharType.VARCHAR, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle DATE_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("date_column_name", DateType.DATE, OptionalInt.empty(), "date_column_name", DateType.DATE, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle NESTED_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("nested_column_name", NESTED_ROW_FIELD, OptionalInt.empty(), "nested_column_name", NESTED_ROW_FIELD, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle EXPECTED_NESTED_COLUMN_HANDLE =
            new DeltaLakeColumnHandle(
                    "nested_column_name",
                    NESTED_ROW_FIELD,
                    OptionalInt.empty(),
                    "nested_column_name",
                    NESTED_ROW_FIELD,
                    REGULAR,
                    Optional.of(new DeltaLakeColumnProjectionInfo(INTEGER, ImmutableList.of(1), ImmutableList.of("child2"))));
    private static final DeltaLakeColumnHandle NESTED_COLUMN_HANDLE_WITH_PROJECTION =
            new DeltaLakeColumnHandle(
                    "highly_nested_column_name",
                    HIGHLY_NESTED_ROW_FIELD,
                    OptionalInt.empty(),
                    "highly_nested_column_name",
                    HIGHLY_NESTED_ROW_FIELD,
                    REGULAR,
                    Optional.of(new DeltaLakeColumnProjectionInfo(INTEGER, ImmutableList.of(0, 0), ImmutableList.of("grandparent", "parent"))));
    private static final DeltaLakeColumnHandle EXPECTED_NESTED_COLUMN_HANDLE_WITH_PROJECTION =
            new DeltaLakeColumnHandle(
                    "highly_nested_column_name",
                    HIGHLY_NESTED_ROW_FIELD,
                    OptionalInt.empty(),
                    "highly_nested_column_name",
                    HIGHLY_NESTED_ROW_FIELD,
                    REGULAR,
                    Optional.of(new DeltaLakeColumnProjectionInfo(INTEGER, ImmutableList.of(0, 0, 0), ImmutableList.of("grandparent", "parent", "child"))));

    private static final Map<String, ColumnHandle> SYNTHETIC_COLUMN_ASSIGNMENTS = ImmutableMap.of(
            "test_synthetic_column_name_1", BOGUS_COLUMN_HANDLE,
            "test_synthetic_column_name_2", VARCHAR_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("nested_column_name", NESTED_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> EXPECTED_NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("nested_column_name#child2", EXPECTED_NESTED_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> HIGHLY_NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("highly_nested_column_name#grandparent#parent", NESTED_COLUMN_HANDLE_WITH_PROJECTION);
    private static final Map<String, ColumnHandle> EXPECTED_HIGHLY_NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("highly_nested_column_name#grandparent#parent#child", EXPECTED_NESTED_COLUMN_HANDLE_WITH_PROJECTION);

    private static final ConnectorExpression DOUBLE_PROJECTION = new Variable("double_projection", DoubleType.DOUBLE);
    private static final ConnectorExpression BOOLEAN_PROJECTION = new Variable("boolean_projection", BooleanType.BOOLEAN);
    private static final ConnectorExpression DEREFERENCE_PROJECTION = new FieldDereference(
            BOGUS_ROW_FIELD,
            new Constant(1, BOGUS_ROW_FIELD),
            0);
    private static final ConnectorExpression NESTED_DEREFERENCE_PROJECTION = new FieldDereference(
            INTEGER,
            new Variable("nested_column_name", NESTED_ROW_FIELD),
            1);
    private static final ConnectorExpression EXPECTED_NESTED_DEREFERENCE_PROJECTION = new Variable(
            "nested_column_name#child2",
            INTEGER);
    private static final ConnectorExpression HIGHLY_NESTED_DEREFERENCE_PROJECTION = new FieldDereference(
            INTEGER,
            new Variable("highly_nested_column_name#grandparent#parent", HIGHLY_NESTED_ROW_FIELD),
            0);
    private static final ConnectorExpression EXPECTED_HIGHLY_NESTED_DEREFERENCE_PROJECTION = new Variable(
            "highly_nested_column_name#grandparent#parent#child",
            INTEGER);

    private static final List<ConnectorExpression> SIMPLE_COLUMN_PROJECTIONS =
            ImmutableList.of(DOUBLE_PROJECTION, BOOLEAN_PROJECTION);
    private static final List<ConnectorExpression> DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(DOUBLE_PROJECTION, DEREFERENCE_PROJECTION, BOOLEAN_PROJECTION);
    private static final List<ConnectorExpression> NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(NESTED_DEREFERENCE_PROJECTION);
    private static final List<ConnectorExpression> EXPECTED_NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(EXPECTED_NESTED_DEREFERENCE_PROJECTION);
    private static final List<ConnectorExpression> HIGHLY_NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(HIGHLY_NESTED_DEREFERENCE_PROJECTION);
    private static final List<ConnectorExpression> EXPECTED_HIGHLY_NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(EXPECTED_HIGHLY_NESTED_DEREFERENCE_PROJECTION);

    private static final Set<DeltaLakeColumnHandle> PREDICATE_COLUMNS =
            ImmutableSet.of(BOOLEAN_COLUMN_HANDLE, DOUBLE_COLUMN_HANDLE);

    private File temporaryCatalogDirectory;
    private DeltaLakeMetadataFactory deltaLakeMetadataFactory;

    @BeforeClass
    public void setUp()
            throws IOException
    {
        temporaryCatalogDirectory = createTempDirectory("HiveCatalog").toFile();
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", temporaryCatalogDirectory.getPath())
                .buildOrThrow();

        Bootstrap app = new Bootstrap(
                // connector dependencies
                new JsonModule(),
                binder -> {
                    ConnectorContext context = new TestingConnectorContext();
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                    binder.bind(CatalogName.class).toInstance(new CatalogName("test"));
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                },
                // connector modules
                new DeltaLakeMetastoreModule(),
                new DeltaLakeModule(),
                // test setup
                binder -> {
                    binder.bind(HdfsEnvironment.class).toInstance(HDFS_ENVIRONMENT);
                    binder.bind(TrinoHdfsFileSystemStats.class).toInstance(HDFS_FILE_SYSTEM_STATS);
                    binder.bind(TrinoFileSystemFactory.class).to(HdfsFileSystemFactory.class).in(Scopes.SINGLETON);
                },
                new AbstractModule()
                {
                    @Provides
                    public DeltaLakeMetastore getDeltaLakeMetastore(@RawHiveMetastoreFactory HiveMetastoreFactory hiveMetastoreFactory)
                    {
                        return new HiveMetastoreBackedDeltaLakeMetastore(hiveMetastoreFactory.createMetastore(Optional.empty()));
                    }
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        deltaLakeMetadataFactory = injector.getInstance(DeltaLakeMetadataFactory.class);

        injector.getInstance(DeltaLakeMetastore.class)
                .createDatabase(Database.builder()
                        .setDatabaseName(DATABASE_NAME)
                        .setOwnerName(Optional.of("test"))
                        .setOwnerType(Optional.of(USER))
                        .setLocation(Optional.empty())
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(() -> deleteRecursively(temporaryCatalogDirectory.toPath(), ALLOW_INSECURE));
        temporaryCatalogDirectory = null;
    }

    @Test
    public void testGetNewTableLayout()
    {
        Optional<ConnectorTableLayout> newTableLayout = deltaLakeMetadataFactory.create(SESSION.getIdentity())
                .getNewTableLayout(
                        SESSION,
                        newTableMetadata(
                                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                                ImmutableList.of(BIGINT_COLUMN_2)));

        assertThat(newTableLayout).isPresent();

        // should not have ConnectorPartitioningHandle since DeltaLake does not support bucketing
        assertThat(newTableLayout.get().getPartitioning()).isNotPresent();

        assertThat(newTableLayout.get().getPartitionColumns())
                .isEqualTo(ImmutableList.of(BIGINT_COLUMN_2.getName()));
    }

    @Test
    public void testGetNewTableLayoutNoPartitionColumns()
    {
        assertThat(deltaLakeMetadataFactory.create(SESSION.getIdentity())
                .getNewTableLayout(
                        SESSION,
                        newTableMetadata(
                                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                                ImmutableList.of())))
                .isNotPresent();
    }

    @Test
    public void testGetNewTableLayoutInvalidPartitionColumns()
    {
        assertThatThrownBy(() -> deltaLakeMetadataFactory.create(SESSION.getIdentity())
                .getNewTableLayout(
                        SESSION,
                        newTableMetadata(
                                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                                ImmutableList.of(BIGINT_COLUMN_2, MISSING_COLUMN))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Table property 'partition_by' contained column names which do not exist: [missing_column]");

        assertThatThrownBy(() -> deltaLakeMetadataFactory.create(SESSION.getIdentity())
                .getNewTableLayout(
                        SESSION,
                        newTableMetadata(
                                ImmutableList.of(TIMESTAMP_COLUMN, BIGINT_COLUMN_2),
                                ImmutableList.of(BIGINT_COLUMN_2))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported type: timestamp(3)");
    }

    @Test
    public void testGetInsertLayout()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(BIGINT_COLUMN_1));

        deltaLakeMetadata.createTable(SESSION, tableMetadata, false);

        Optional<ConnectorTableLayout> insertLayout = deltaLakeMetadata
                .getInsertLayout(
                        SESSION,
                        deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable()));

        assertThat(insertLayout).isPresent();

        assertThat(insertLayout.get().getPartitioning()).isNotPresent();

        assertThat(insertLayout.get().getPartitionColumns())
                .isEqualTo(getPartitionColumnNames(ImmutableList.of(BIGINT_COLUMN_1)));
    }

    private ConnectorTableMetadata newTableMetadata(List<ColumnMetadata> tableColumns, List<ColumnMetadata> partitionTableColumns)
    {
        return new ConnectorTableMetadata(
                newMockSchemaTableName(),
                tableColumns,
                ImmutableMap.of(
                        PARTITIONED_BY_PROPERTY,
                        getPartitionColumnNames(partitionTableColumns)));
    }

    @Test
    public void testGetInsertLayoutTableUnpartitioned()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1),
                ImmutableList.of());

        deltaLakeMetadata.createTable(SESSION, tableMetadata, false);

        // should return empty insert layout since table exists but is unpartitioned
        assertThat(deltaLakeMetadata.getInsertLayout(
                SESSION,
                deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable())))
                .isNotPresent();
    }

    @DataProvider
    public Object[][] testApplyProjectionProvider()
    {
        return new Object[][] {
                {
                        ImmutableSet.of(),
                        SYNTHETIC_COLUMN_ASSIGNMENTS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                        SYNTHETIC_COLUMN_ASSIGNMENTS
                },
                {
                        // table handle already contains subset of expected projected columns
                        ImmutableSet.of(BOGUS_COLUMN_HANDLE),
                        SYNTHETIC_COLUMN_ASSIGNMENTS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                        SYNTHETIC_COLUMN_ASSIGNMENTS
                },
                {
                        // table handle already contains superset of expected projected columns
                        ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                        SYNTHETIC_COLUMN_ASSIGNMENTS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                        SYNTHETIC_COLUMN_ASSIGNMENTS
                },
                {
                        // table handle has empty assignments
                        ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                        ImmutableMap.of(),
                        SIMPLE_COLUMN_PROJECTIONS,
                        SIMPLE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(),
                        ImmutableMap.of()
                },
                {
                        ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                        ImmutableMap.of(),
                        DEREFERENCE_COLUMN_PROJECTIONS,
                        DEREFERENCE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(),
                        ImmutableMap.of()
                },
                {
                        ImmutableSet.of(NESTED_COLUMN_HANDLE),
                        NESTED_COLUMN_ASSIGNMENTS,
                        NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                        EXPECTED_NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(EXPECTED_NESTED_COLUMN_HANDLE),
                        EXPECTED_NESTED_COLUMN_ASSIGNMENTS
                },
                {
                        ImmutableSet.of(HIGHLY_NESTED_ROW_FIELD),
                        HIGHLY_NESTED_COLUMN_ASSIGNMENTS,
                        HIGHLY_NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                        EXPECTED_HIGHLY_NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                        ImmutableSet.of(EXPECTED_NESTED_COLUMN_HANDLE_WITH_PROJECTION),
                        EXPECTED_HIGHLY_NESTED_COLUMN_ASSIGNMENTS
                }
        };
    }

    @Test(dataProvider = "testApplyProjectionProvider")
    public void testApplyProjection(
            Set<DeltaLakeColumnHandle> inputProjectedColumns,
            Map<String, ColumnHandle> inputAssignments,
            List<ConnectorExpression> inputProjections,
            List<ConnectorExpression> expectedProjections,
            Set<DeltaLakeColumnHandle> expectedProjectedColumns,
            Map<String, ColumnHandle> expectedAssignments)
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ProjectionApplicationResult<ConnectorTableHandle> projection = deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(inputProjectedColumns, PREDICATE_COLUMNS),
                        inputProjections,
                        inputAssignments)
                .get();

        assertThat(((DeltaLakeTableHandle) projection.getHandle()).getProjectedColumns())
                .isEqualTo(Optional.of(expectedProjectedColumns));

        assertThat(projection.getProjections())
                .usingRecursiveComparison()
                .isEqualTo(expectedProjections);

        assertThat(projection.getAssignments())
                .usingRecursiveComparison()
                .isEqualTo(createNewColumnAssignments(expectedAssignments));

        assertThat(projection.isPrecalculateStatistics())
                .isFalse();
    }

    @Test
    public void testApplyProjectionWithEmptyResult()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        assertThat(deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(
                                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                                PREDICATE_COLUMNS),
                        SIMPLE_COLUMN_PROJECTIONS,
                        SYNTHETIC_COLUMN_ASSIGNMENTS))
                .isEmpty();

        assertThat(deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(ImmutableSet.of(), ImmutableSet.of()),
                        ImmutableList.of(),
                        ImmutableMap.of()))
                .isEmpty();
    }

    @Test
    public void testGetInputInfoForPartitionedTable()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(BIGINT_COLUMN_1));
        deltaLakeMetadata.createTable(SESSION, tableMetadata, false);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable());
        assertThat(deltaLakeMetadata.getInfo(tableHandle)).isEqualTo(Optional.of(new DeltaLakeInputInfo(true)));
    }

    @Test
    public void testGetInputInfoForUnPartitionedTable()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of());
        deltaLakeMetadata.createTable(SESSION, tableMetadata, false);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable());
        assertThat(deltaLakeMetadata.getInfo(tableHandle)).isEqualTo(Optional.of(new DeltaLakeInputInfo(false)));
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(Set<DeltaLakeColumnHandle> projectedColumns, Set<DeltaLakeColumnHandle> constrainedColumns)
    {
        return new DeltaLakeTableHandle(
                "test_schema_name",
                "test_table_name",
                true,
                "test_location",
                createMetadataEntry(),
                createConstrainedColumnsTuple(constrainedColumns),
                TupleDomain.all(),
                Optional.of(DeltaLakeTableHandle.WriteType.UPDATE),
                Optional.of(projectedColumns),
                Optional.of(ImmutableList.of(BOOLEAN_COLUMN_HANDLE)),
                Optional.of(ImmutableList.of(DOUBLE_COLUMN_HANDLE)),
                Optional.empty(),
                0);
    }

    private static TupleDomain<DeltaLakeColumnHandle> createConstrainedColumnsTuple(
            Set<DeltaLakeColumnHandle> constrainedColumns)
    {
        ImmutableMap.Builder<DeltaLakeColumnHandle, Domain> tupleBuilder = ImmutableMap.builder();

        constrainedColumns.forEach(column -> {
            verify(column.isBaseColumn(), "Unexpected dereference: %s", column);
            tupleBuilder.put(column, Domain.notNull(column.getBaseType()));
        });

        return TupleDomain.withColumnDomains(tupleBuilder.buildOrThrow());
    }

    private static List<Assignment> createNewColumnAssignments(Map<String, ColumnHandle> assignments)
    {
        return assignments.entrySet().stream()
                .map(assignment -> {
                    DeltaLakeColumnHandle column = ((DeltaLakeColumnHandle) assignment.getValue());
                    Type type = column.getProjectionInfo().map(DeltaLakeColumnProjectionInfo::getType).orElse(column.getBaseType());
                    return new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            type);
                })
                .collect(toImmutableList());
    }

    private static MetadataEntry createMetadataEntry()
    {
        return new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                "test_schema",
                ImmutableList.of("test_partition_column"),
                ImmutableMap.of("test_configuration_key", "test_configuration_value"),
                1);
    }

    private static List<String> getPartitionColumnNames(List<ColumnMetadata> tableMetadataColumns)
    {
        return tableMetadataColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
    }

    private static SchemaTableName newMockSchemaTableName()
    {
        String randomSuffix = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return new SchemaTableName(DATABASE_NAME, "table_" + randomSuffix);
    }
}
