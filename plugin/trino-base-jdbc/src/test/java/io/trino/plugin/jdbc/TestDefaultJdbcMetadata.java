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
package io.trino.plugin.jdbc;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.DefaultJdbcMetadata.createSyntheticAggregationColumn;
import static io.trino.plugin.jdbc.DefaultJdbcMetadata.createSyntheticJoinProjectionColumn;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestDefaultJdbcMetadata
{
    private TestingDatabase database;
    private DefaultJdbcMetadata metadata;
    private JdbcTableHandle tableHandle;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(database.getJdbcClient(),
                Optional.empty()),
                TimestampTimeZoneDomain.ANY,
                false,
                ImmutableSet.of());
        tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSupportsRetriesValidation()
    {
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(database.getJdbcClient(),
                Optional.of(false)),
                TimestampTimeZoneDomain.ANY,
                false,
                ImmutableSet.of());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(new SchemaTableName("example", "numbers"), ImmutableList.of());

        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(), RetryMode.RETRIES_ENABLED, false))
                .hasMessageContaining("This connector does not support query or task retries");

        assertThatThrownBy(() -> metadata.beginInsert(SESSION, tableHandle, ImmutableList.of(), RetryMode.RETRIES_ENABLED))
                .hasMessageContaining("This connector does not support query or task retries");
    }

    @Test
    public void testNonTransactionalInsertValidation()
    {
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(database.getJdbcClient(),
                Optional.of(true)),
                TimestampTimeZoneDomain.ANY,
                false,
                ImmutableSet.of());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(new SchemaTableName("example", "numbers"), ImmutableList.of());

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(ImmutableList.of(
                        PropertyMetadata.booleanProperty(JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT, "description", true, false)))
                .build();

        assertThatThrownBy(() -> metadata.beginCreateTable(session, tableMetadata, Optional.empty(), RetryMode.RETRIES_ENABLED, false))
                .hasMessageContaining("Query and task retries are incompatible with non-transactional inserts");

        assertThatThrownBy(() -> metadata.beginInsert(session, tableHandle, ImmutableList.of(), RetryMode.RETRIES_ENABLED))
                .hasMessageContaining("Query and task retries are incompatible with non-transactional inserts");
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        database.close();
        database = null;
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsAll(ImmutableSet.of("example", "tpch"));
    }

    @Test
    public void testGetTableHandle()
    {
        JdbcTableHandle tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty())).isEqualTo(tableHandle);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertThat(metadata.getColumnHandles(SESSION, tableHandle)).isEqualTo(ImmutableMap.of(
                "text", new JdbcColumnHandle("TEXT", JDBC_VARCHAR, VARCHAR),
                "text_short", new JdbcColumnHandle("TEXT_SHORT", JDBC_VARCHAR, createVarcharType(32)),
                "value", new JdbcColumnHandle("VALUE", JDBC_BIGINT, BIGINT)));

        // unknown table
        unknownTableColumnHandle(new JdbcTableHandle(new SchemaTableName("unknown", "unknown"), new RemoteTableName(Optional.of("unknown"), Optional.of("unknown"), "unknown"), Optional.empty()));
        unknownTableColumnHandle(new JdbcTableHandle(new SchemaTableName("example", "numbers"), new RemoteTableName(Optional.empty(), Optional.of("example"), "unknown"), Optional.empty()));
    }

    private void unknownTableColumnHandle(JdbcTableHandle tableHandle)
    {
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, tableHandle))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table '%s' has no supported columns (all 0 columns are not supported)", tableHandle.asPlainTable().getSchemaTableName());
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("example", "numbers"));
        assertThat(tableMetadata.getColumns()).isEqualTo(ImmutableList.of(
                ColumnMetadata.builder().setName("text").setType(VARCHAR).setNullable(false).build(), // primary key is not null in H2
                new ColumnMetadata("text_short", createVarcharType(32)),
                new ColumnMetadata("value", BIGINT)));

        // escaping name patterns
        JdbcTableHandle specialTableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("exa_ple", "num_ers"), Optional.empty(), Optional.empty());
        ConnectorTableMetadata specialTableMetadata = metadata.getTableMetadata(SESSION, specialTableHandle);
        assertThat(specialTableMetadata.getTable()).isEqualTo(new SchemaTableName("exa_ple", "num_ers"));
        assertThat(specialTableMetadata.getColumns()).isEqualTo(ImmutableList.of(
                ColumnMetadata.builder().setName("te_t").setType(VARCHAR).setNullable(false).build(), // primary key is not null in H2
                new ColumnMetadata("va%ue", BIGINT)));

        // unknown tables should produce null
        unknownTableMetadata(new JdbcTableHandle(new SchemaTableName("u", "numbers"), new RemoteTableName(Optional.empty(), Optional.of("unknown"), "unknown"), Optional.empty()));
        unknownTableMetadata(new JdbcTableHandle(new SchemaTableName("example", "numbers"), new RemoteTableName(Optional.empty(), Optional.of("example"), "unknown"), Optional.empty()));
        unknownTableMetadata(new JdbcTableHandle(new SchemaTableName("example", "numbers"), new RemoteTableName(Optional.empty(), Optional.of("unknown"), "numbers"), Optional.empty()));
    }

    private void unknownTableMetadata(JdbcTableHandle tableHandle)
    {
        assertThatThrownBy(() -> metadata.getTableMetadata(SESSION, tableHandle))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table '%s' has no supported columns (all 0 columns are not supported)", tableHandle.asPlainTable().getSchemaTableName());
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(metadata.listTables(SESSION, Optional.empty())).containsOnly(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "timestamps"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("exa_ple", "table_with_float_col"),
                new SchemaTableName("exa_ple", "num_ers"));

        // specific schema
        assertThat(metadata.listTables(SESSION, Optional.of("example")))
                .containsOnly(
                        new SchemaTableName("example", "numbers"),
                        new SchemaTableName("example", "timestamps"),
                        new SchemaTableName("example", "view_source"),
                        new SchemaTableName("example", "view"));

        assertThat(metadata.listTables(SESSION, Optional.of("tpch")))
                .containsOnly(
                        new SchemaTableName("tpch", "orders"),
                        new SchemaTableName("tpch", "lineitem"));

        assertThat(metadata.listTables(SESSION, Optional.of("exa_ple")))
                .containsOnly(
                        new SchemaTableName("exa_ple", "num_ers"),
                        new SchemaTableName("exa_ple", "table_with_float_col"));

        // unknown schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown")))).isEmpty();
    }

    @Test
    public void getColumnMetadata()
    {
        assertThat(metadata.getColumnMetadata(SESSION, tableHandle, new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR))).isEqualTo(new ColumnMetadata("text", VARCHAR));
    }

    @Test
    public void testCreateAndAlterTable()
    {
        SchemaTableName table = new SchemaTableName("example", "foo");
        metadata.createTable(SESSION, new ConnectorTableMetadata(table, ImmutableList.of(new ColumnMetadata("text", VARCHAR))), SaveMode.FAIL);

        JdbcTableHandle handle = metadata.getTableHandle(SESSION, table, Optional.empty(), Optional.empty());

        ConnectorTableMetadata layout = metadata.getTableMetadata(SESSION, handle);
        assertThat(layout.getTable()).isEqualTo(table);
        assertThat(layout.getColumns())
                .containsExactly(new ColumnMetadata("text", VARCHAR));

        metadata.addColumn(SESSION, handle, new ColumnMetadata("x", VARCHAR), new ColumnPosition.Last());
        layout = metadata.getTableMetadata(SESSION, handle);
        assertThat(layout.getColumns())
                .containsExactly(
                        new ColumnMetadata("text", VARCHAR),
                        new ColumnMetadata("x", VARCHAR));

        JdbcColumnHandle columnHandle = new JdbcColumnHandle("x", JDBC_VARCHAR, VARCHAR);
        metadata.dropColumn(SESSION, handle, columnHandle);
        layout = metadata.getTableMetadata(SESSION, handle);
        assertThat(layout.getColumns())
                .containsExactly(new ColumnMetadata("text", VARCHAR));

        SchemaTableName newTableName = new SchemaTableName("example", "bar");
        metadata.renameTable(SESSION, handle, newTableName);
        handle = metadata.getTableHandle(SESSION, newTableName, Optional.empty(), Optional.empty());
        layout = metadata.getTableMetadata(SESSION, handle);
        assertThat(layout.getTable()).isEqualTo(newTableName);
        assertThat(layout.getColumns())
                .containsExactly(new ColumnMetadata("text", VARCHAR));
    }

    @Test
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, tableHandle);

        assertTrinoExceptionThrownBy(() -> metadata.getTableMetadata(SESSION, tableHandle))
                .hasErrorCode(NOT_FOUND);
    }

    @Test
    public void testAggregationPushdownForTableHandle()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        ColumnHandle groupByColumn = metadata.getColumnHandles(session, tableHandle).get("text");
        Function<ConnectorTableHandle, Optional<AggregationApplicationResult<ConnectorTableHandle>>> applyAggregation = handle -> metadata.applyAggregation(
                session,
                handle,
                ImmutableList.of(new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty())),
                ImmutableMap.of(),
                ImmutableList.of(ImmutableList.of(groupByColumn)));

        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());
        Optional<AggregationApplicationResult<ConnectorTableHandle>> aggregationResult = applyAggregation.apply(baseTableHandle);
        assertThat(aggregationResult).isPresent();

        SchemaTableName noAggregationPushdownTable = new SchemaTableName("example", "no_aggregation_pushdown");
        metadata.createTable(SESSION, new ConnectorTableMetadata(noAggregationPushdownTable, ImmutableList.of(new ColumnMetadata("text", VARCHAR))), SaveMode.FAIL);
        ConnectorTableHandle noAggregationPushdownTableHandle = metadata.getTableHandle(session, noAggregationPushdownTable, Optional.empty(), Optional.empty());
        aggregationResult = applyAggregation.apply(noAggregationPushdownTableHandle);
        assertThat(aggregationResult).isEmpty();
    }

    @Test
    public void testApplyFilterAfterAggregationPushdown()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        ColumnHandle groupByColumn = metadata.getColumnHandles(session, tableHandle).get("text");
        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());
        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, baseTableHandle, ImmutableList.of(ImmutableList.of(groupByColumn)));

        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("one"));
        JdbcTableHandle tableHandleWithFilter = applyFilter(session, aggregatedTable, new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(groupByColumn, domain))));

        assertThat(tableHandleWithFilter.getConstraint().getDomains()).isEqualTo(Optional.of(ImmutableMap.of(groupByColumn, domain)));
    }

    @Test
    public void testCombineFiltersWithAggregationPushdown()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        ColumnHandle groupByColumn = metadata.getColumnHandles(session, tableHandle).get("text");
        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());

        Domain firstDomain = Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("one"), utf8Slice("two")));
        JdbcTableHandle filterResult = applyFilter(session, baseTableHandle, new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(groupByColumn, firstDomain))));

        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, filterResult, ImmutableList.of(ImmutableList.of(groupByColumn)));

        Domain secondDomain = Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("one"), utf8Slice("three")));
        JdbcTableHandle tableHandleWithFilter = applyFilter(session, aggregatedTable, new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(groupByColumn, secondDomain))));
        assertThat(tableHandleWithFilter.getConstraint().getDomains())
                .isEqualTo(
                    // The query effectively intersects firstDomain and secondDomain, but this is not visible in JdbcTableHandle.constraint,
                    // as firstDomain has been converted into a PreparedQuery
                    Optional.of(ImmutableMap.of(groupByColumn, secondDomain)));
        assertThat(((JdbcQueryRelationHandle) tableHandleWithFilter.getRelationHandle()).getPreparedQuery().query())
                .isEqualTo("SELECT \"TEXT\", count(*) AS \"_pfgnrtd_0\" " +
                        "FROM \"" + database.getDatabaseName() + "\".\"EXAMPLE\".\"NUMBERS\" " +
                        "WHERE \"TEXT\" IN (?,?) " +
                        "GROUP BY \"TEXT\"");
    }

    @Test
    public void testNonGroupKeyPredicatePushdown()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle groupByColumn = columnHandles.get("text");
        ColumnHandle nonGroupByColumn = columnHandles.get("value");

        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());
        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, baseTableHandle, ImmutableList.of(ImmutableList.of(groupByColumn)));

        Domain domain = Domain.singleValue(BIGINT, 123L);
        JdbcTableHandle tableHandleWithFilter = applyFilter(
                session,
                aggregatedTable,
                new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(nonGroupByColumn, domain))));
        assertThat(tableHandleWithFilter.getConstraint().getDomains()).isEqualTo(Optional.of(ImmutableMap.of(nonGroupByColumn, domain)));
        assertThat(((JdbcQueryRelationHandle) tableHandleWithFilter.getRelationHandle()).getPreparedQuery().query())
                .isEqualTo("SELECT \"TEXT\", count(*) AS \"_pfgnrtd_0\" " +
                        "FROM \"" + database.getDatabaseName() + "\".\"EXAMPLE\".\"NUMBERS\" " +
                        "GROUP BY \"TEXT\"");
    }

    @Test
    public void testMultiGroupKeyPredicatePushdown()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle textColumn = columnHandles.get("text");
        ColumnHandle valueColumn = columnHandles.get("value");

        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());

        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, baseTableHandle, ImmutableList.of(ImmutableList.of(textColumn, valueColumn), ImmutableList.of(textColumn)));

        Domain domain = Domain.singleValue(BIGINT, 123L);
        JdbcTableHandle tableHandleWithFilter = applyFilter(
                session,
                aggregatedTable,
                new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(valueColumn, domain))));
        assertThat(tableHandleWithFilter.getConstraint().getDomains()).isEqualTo(Optional.of(ImmutableMap.of(valueColumn, domain)));
        assertThat(((JdbcQueryRelationHandle) tableHandleWithFilter.getRelationHandle()).getPreparedQuery().query())
                .isEqualTo("SELECT \"TEXT\", \"VALUE\", count(*) AS \"_pfgnrtd_0\" " +
                        "FROM \"" + database.getDatabaseName() + "\".\"EXAMPLE\".\"NUMBERS\" " +
                        "GROUP BY GROUPING SETS ((\"TEXT\", \"VALUE\"), (\"TEXT\"))");
    }

    @Test
    public void testApplyTableScanRedirect()
    {
        TableScanRedirectApplicationResult tableScanRedirectApplicationResult = new TableScanRedirectApplicationResult(
                new CatalogSchemaTableName("target_catalog", "targert_schema", "target_table"),
                ImmutableMap.of(),
                TupleDomain.all());
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(
                database.getJdbcClient(),
                Optional.empty(),
                Optional.of(tableScanRedirectApplicationResult)),
                TimestampTimeZoneDomain.ANY,
                false,
                ImmutableSet.of());

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        JdbcTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"), Optional.empty(), Optional.empty());

        // redirection is applied if constraintExpressions is empty
        assertThat(metadata.applyTableScanRedirect(session, baseTableHandle)).hasValueSatisfying(actualResult -> {
            assertThat(actualResult.getDestinationTable()).isEqualTo(tableScanRedirectApplicationResult.getDestinationTable());
            assertThat(actualResult.getDestinationColumns()).isEmpty();
            assertThat(actualResult.getFilter()).isEqualTo(TupleDomain.all());
        });

        JdbcTableHandle filterWithConstraintExpressionResult = new JdbcTableHandle(
                baseTableHandle.getRelationHandle(),
                TupleDomain.all(),
                ImmutableList.of(new ParameterizedExpression("like", ImmutableList.of())),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(ImmutableSet.of()),
                0,
                Optional.empty(),
                ImmutableList.of());

        // redirection is not applied if constraintExpressions is present
        assertThat(metadata.applyTableScanRedirect(session, filterWithConstraintExpressionResult)).isEmpty();
    }

    @Test
    public void testColumnAliasTruncation()
    {
        OptionalInt maxLength = OptionalInt.of(30);
        assertThat(createSyntheticJoinProjectionColumn(column("no_truncation"), 123, maxLength).getColumnName())
                .isEqualTo("no_truncation_123");
        assertThat(createSyntheticJoinProjectionColumn(column("long_column_name_gets_truncated"), 123, maxLength).getColumnName())
                .isEqualTo("long_column_name_gets_trun_123");
        assertThat(createSyntheticJoinProjectionColumn(column("long_id_causes_truncation"), Integer.MAX_VALUE, maxLength).getColumnName())
                .isEqualTo("long_id_causes_trun_2147483647");

        assertThat(createSyntheticJoinProjectionColumn(column("id_equals_max_length"), 1234, OptionalInt.of(4)).getColumnName())
                .isEqualTo("1234");
        assertThat(createSyntheticJoinProjectionColumn(column("id_and_separator_equals_max_length"), 1234, OptionalInt.of(5)).getColumnName())
                .isEqualTo("_1234");
    }

    @Test
    public void testSyntheticIdExceedsLength()
    {
        assertThatThrownBy(() -> createSyntheticJoinProjectionColumn(column("id_exceeds_max_length"), 1234, OptionalInt.of(3)))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Maximum allowed column name length is 3 but next synthetic id has length 4");
    }

    @Test
    public void testNegativeSyntheticId()
    {
        //noinspection NumericOverflow
        assertThatThrownBy(() -> createSyntheticJoinProjectionColumn(column("negative_id"), Integer.MAX_VALUE + 1, OptionalInt.of(30)))
                .isInstanceOf(VerifyException.class)
                .hasMessage("nextSyntheticColumnId rolled over and is not monotonically increasing any more");
    }

    @Test
    public void testAggregationColumnAliasMaxLength()
    {
        // this test is to ensure that the generated names for aggregation pushdown are short enough to be supported by all databases.
        // Oracle has the smallest limit at 30 so any value smaller than that is acceptable.
        assertThat(createSyntheticAggregationColumn(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty()),
                JDBC_BIGINT,
                Integer.MAX_VALUE).getColumnName().length())
                .isEqualTo(19);
    }

    private static JdbcColumnHandle column(String columnName)
    {
        return JdbcColumnHandle.builder()
                .setJdbcTypeHandle(JDBC_VARCHAR)
                .setColumnType(VARCHAR)
                .setColumnName(columnName)
                .build();
    }

    private JdbcTableHandle applyCountAggregation(ConnectorSession session, ConnectorTableHandle tableHandle, List<List<ColumnHandle>> groupByColumns)
    {
        Optional<AggregationApplicationResult<ConnectorTableHandle>> aggResult = metadata.applyAggregation(
                session,
                tableHandle,
                ImmutableList.of(new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty())),
                ImmutableMap.of(),
                groupByColumns);
        assertThat(aggResult).isPresent();
        return (JdbcTableHandle) aggResult.get().getHandle();
    }

    private JdbcTableHandle applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> filterResult = metadata.applyFilter(
                session,
                tableHandle,
                constraint);
        assertThat(filterResult).isPresent();
        return (JdbcTableHandle) filterResult.get().getHandle();
    }

    private static class GroupingSetsEnabledJdbcClient
            extends ForwardingJdbcClient
    {
        private final JdbcClient delegate;
        private final Optional<Boolean> supportsRetriesOverride;
        private final Optional<TableScanRedirectApplicationResult> tableScanRedirectApplicationResult;

        public GroupingSetsEnabledJdbcClient(JdbcClient jdbcClient, Optional<Boolean> supportsRetriesOverride)
        {
            this(jdbcClient, supportsRetriesOverride, Optional.empty());
        }

        public GroupingSetsEnabledJdbcClient(JdbcClient jdbcClient, Optional<Boolean> supportsRetriesOverride, Optional<TableScanRedirectApplicationResult> tableScanRedirectApplicationResult)
        {
            this.delegate = jdbcClient;
            this.supportsRetriesOverride = supportsRetriesOverride;
            this.tableScanRedirectApplicationResult = requireNonNull(tableScanRedirectApplicationResult, "tableScanRedirectApplicationResult is null");
        }

        @Override
        protected JdbcClient delegate()
        {
            return delegate;
        }

        @Override
        public boolean supportsRetries()
        {
            return supportsRetriesOverride.orElseGet(super::supportsRetries);
        }

        @Override
        public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle)
        {
            return tableScanRedirectApplicationResult;
        }

        @Override
        public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
        {
            // disable aggregation pushdown for any table named no_agg_pushdown
            return !"no_aggregation_pushdown".equalsIgnoreCase(table.getRequiredNamedRelation().getRemoteTableName().getTableName());
        }
    }
}
