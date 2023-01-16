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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDefaultJdbcMetadata
{
    private TestingDatabase database;
    private DefaultJdbcMetadata metadata;
    private JdbcTableHandle tableHandle;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(database.getJdbcClient(), Optional.empty()), false, ImmutableSet.of());
        tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
    }

    @Test
    public void testSupportsRetriesValidation()
    {
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(database.getJdbcClient(), Optional.of(false)), false, ImmutableSet.of());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(new SchemaTableName("example", "numbers"), ImmutableList.of());

        assertThatThrownBy(() -> {
            metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(), RetryMode.RETRIES_ENABLED);
        }).hasMessageContaining("This connector does not support query or task retries");

        assertThatThrownBy(() -> {
            metadata.beginInsert(SESSION, tableHandle, ImmutableList.of(), RetryMode.RETRIES_ENABLED);
        }).hasMessageContaining("This connector does not support query or task retries");
    }

    @Test
    public void testNonTransactionalInsertValidation()
    {
        metadata = new DefaultJdbcMetadata(new GroupingSetsEnabledJdbcClient(database.getJdbcClient(), Optional.of(true)), false, ImmutableSet.of());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(new SchemaTableName("example", "numbers"), ImmutableList.of());

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(ImmutableList.of(
                        PropertyMetadata.booleanProperty(JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT, "description", true, false)))
                .build();

        assertThatThrownBy(() -> {
            metadata.beginCreateTable(session, tableMetadata, Optional.empty(), RetryMode.RETRIES_ENABLED);
        }).hasMessageContaining("Query and task retries are incompatible with non-transactional inserts");

        assertThatThrownBy(() -> {
            metadata.beginInsert(session, tableHandle, ImmutableList.of(), RetryMode.RETRIES_ENABLED);
        }).hasMessageContaining("Query and task retries are incompatible with non-transactional inserts");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
        database = null;
    }

    @Test
    public void testListSchemaNames()
    {
        assertTrue(metadata.listSchemaNames(SESSION).containsAll(ImmutableSet.of("example", "tpch")));
    }

    @Test
    public void testGetTableHandle()
    {
        JdbcTableHandle tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers")), tableHandle);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, tableHandle), ImmutableMap.of(
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
        assertEquals(tableMetadata.getTable(), new SchemaTableName("example", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                ColumnMetadata.builder().setName("text").setType(VARCHAR).setNullable(false).build(), // primary key is not null in H2
                new ColumnMetadata("text_short", createVarcharType(32)),
                new ColumnMetadata("value", BIGINT)));

        // escaping name patterns
        JdbcTableHandle specialTableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("exa_ple", "num_ers"));
        ConnectorTableMetadata specialTableMetadata = metadata.getTableMetadata(SESSION, specialTableHandle);
        assertEquals(specialTableMetadata.getTable(), new SchemaTableName("exa_ple", "num_ers"));
        assertEquals(specialTableMetadata.getColumns(), ImmutableList.of(
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
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "timestamps"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("exa_ple", "table_with_float_col"),
                new SchemaTableName("exa_ple", "num_ers")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("example"))), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "timestamps"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("tpch"))), ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("exa_ple"))), ImmutableSet.of(
                new SchemaTableName("exa_ple", "num_ers"),
                new SchemaTableName("exa_ple", "table_with_float_col")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(
                metadata.getColumnMetadata(SESSION, tableHandle, new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR)),
                new ColumnMetadata("text", VARCHAR));
    }

    @Test
    public void testCreateAndAlterTable()
    {
        SchemaTableName table = new SchemaTableName("example", "foo");
        metadata.createTable(SESSION, new ConnectorTableMetadata(table, ImmutableList.of(new ColumnMetadata("text", VARCHAR))), false);

        JdbcTableHandle handle = metadata.getTableHandle(SESSION, table);

        ConnectorTableMetadata layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getTable(), table);
        assertEquals(layout.getColumns().size(), 1);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("text", VARCHAR));

        metadata.addColumn(SESSION, handle, new ColumnMetadata("x", VARCHAR));
        layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getColumns().size(), 2);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("text", VARCHAR));
        assertEquals(layout.getColumns().get(1), new ColumnMetadata("x", VARCHAR));

        JdbcColumnHandle columnHandle = new JdbcColumnHandle("x", JDBC_VARCHAR, VARCHAR);
        metadata.dropColumn(SESSION, handle, columnHandle);
        layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getColumns().size(), 1);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("text", VARCHAR));

        SchemaTableName newTableName = new SchemaTableName("example", "bar");
        metadata.renameTable(SESSION, handle, newTableName);
        handle = metadata.getTableHandle(SESSION, newTableName);
        layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getTable(), newTableName);
        assertEquals(layout.getColumns().size(), 1);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("text", VARCHAR));
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

        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"));
        Optional<AggregationApplicationResult<ConnectorTableHandle>> aggregationResult = applyAggregation.apply(baseTableHandle);
        assertThat(aggregationResult).isPresent();

        SchemaTableName noAggregationPushdownTable = new SchemaTableName("example", "no_aggregation_pushdown");
        metadata.createTable(SESSION, new ConnectorTableMetadata(noAggregationPushdownTable, ImmutableList.of(new ColumnMetadata("text", VARCHAR))), false);
        ConnectorTableHandle noAggregationPushdownTableHandle = metadata.getTableHandle(session, noAggregationPushdownTable);
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
        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"));
        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, baseTableHandle, ImmutableList.of(ImmutableList.of(groupByColumn)));

        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("one"));
        JdbcTableHandle tableHandleWithFilter = applyFilter(session, aggregatedTable, new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(groupByColumn, domain))));

        assertEquals(tableHandleWithFilter.getConstraint().getDomains(), Optional.of(ImmutableMap.of(groupByColumn, domain)));
    }

    @Test
    public void testCombineFiltersWithAggregationPushdown()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig().setAggregationPushdownEnabled(true), Optional.empty()).getSessionProperties())
                .build();
        ColumnHandle groupByColumn = metadata.getColumnHandles(session, tableHandle).get("text");
        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"));

        Domain firstDomain = Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("one"), utf8Slice("two")));
        JdbcTableHandle filterResult = applyFilter(session, baseTableHandle, new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(groupByColumn, firstDomain))));

        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, filterResult, ImmutableList.of(ImmutableList.of(groupByColumn)));

        Domain secondDomain = Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("one"), utf8Slice("three")));
        JdbcTableHandle tableHandleWithFilter = applyFilter(session, aggregatedTable, new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(groupByColumn, secondDomain))));
        assertEquals(
                tableHandleWithFilter.getConstraint().getDomains(),
                // The query effectively intersects firstDomain and secondDomain, but this is not visible in JdbcTableHandle.constraint,
                // as firstDomain has been converted into a PreparedQuery
                Optional.of(ImmutableMap.of(groupByColumn, secondDomain)));
        assertEquals(
                ((JdbcQueryRelationHandle) tableHandleWithFilter.getRelationHandle()).getPreparedQuery().getQuery(),
                "SELECT \"TEXT\", count(*) AS \"_pfgnrtd_0\" " +
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

        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"));
        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, baseTableHandle, ImmutableList.of(ImmutableList.of(groupByColumn)));

        Domain domain = Domain.singleValue(BIGINT, 123L);
        JdbcTableHandle tableHandleWithFilter = applyFilter(
                session,
                aggregatedTable,
                new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(nonGroupByColumn, domain))));
        assertEquals(
                tableHandleWithFilter.getConstraint().getDomains(),
                Optional.of(ImmutableMap.of(nonGroupByColumn, domain)));
        assertEquals(
                ((JdbcQueryRelationHandle) tableHandleWithFilter.getRelationHandle()).getPreparedQuery().getQuery(),
                "SELECT \"TEXT\", count(*) AS \"_pfgnrtd_0\" " +
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

        ConnectorTableHandle baseTableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"));

        ConnectorTableHandle aggregatedTable = applyCountAggregation(session, baseTableHandle, ImmutableList.of(ImmutableList.of(textColumn, valueColumn), ImmutableList.of(textColumn)));

        Domain domain = Domain.singleValue(BIGINT, 123L);
        JdbcTableHandle tableHandleWithFilter = applyFilter(
                session,
                aggregatedTable,
                new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(valueColumn, domain))));
        assertEquals(
                tableHandleWithFilter.getConstraint().getDomains(),
                Optional.of(ImmutableMap.of(valueColumn, domain)));
        assertEquals(
                ((JdbcQueryRelationHandle) tableHandleWithFilter.getRelationHandle()).getPreparedQuery().getQuery(),
                "SELECT \"TEXT\", \"VALUE\", count(*) AS \"_pfgnrtd_0\" " +
                        "FROM \"" + database.getDatabaseName() + "\".\"EXAMPLE\".\"NUMBERS\" " +
                        "GROUP BY GROUPING SETS ((\"TEXT\", \"VALUE\"), (\"TEXT\"))");
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

        public GroupingSetsEnabledJdbcClient(JdbcClient jdbcClient, Optional<Boolean> supportsRetriesOverride)
        {
            this.delegate = jdbcClient;
            this.supportsRetriesOverride = supportsRetriesOverride;
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
        public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
        {
            // disable aggregation pushdown for any table named no_agg_pushdown
            return !"no_aggregation_pushdown".equalsIgnoreCase(table.getRequiredNamedRelation().getRemoteTableName().getTableName());
        }
    }
}
