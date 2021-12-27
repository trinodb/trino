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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorInsertTableHandle;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.MetadataManager.MAX_TABLE_REDIRECTIONS;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTableRedirection
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "test_catalog";
    private static final String SCHEMA_ONE = "test_schema_1";
    private static final String SCHEMA_TWO = "test_schema_2";
    private static final String SCHEMA_THREE = "test_schema_3";
    private static final List<String> SCHEMAS = ImmutableList.of(SCHEMA_ONE, SCHEMA_TWO, SCHEMA_THREE);
    private static final String TABLE_FOO = "table_foo";
    private static final String TABLE_BAR = "table_bar";
    private static final String VALID_REDIRECTION_SRC = "valid_redirection_src";
    private static final String VALID_REDIRECTION_TARGET = "valid_redirection_target";
    private static final String BAD_REDIRECTION_SRC = "bad_redirection_src";
    private static final String NON_EXISTENT_TABLE = "non_existent_table";
    private static final String REDIRECTION_TWICE_SRC = "redirection_twice_src";
    private static final String INTERMEDIATE_TABLE = "intermediate_table";
    private static final String REDIRECTION_LOOP_PING = "redirection_loop_ping";
    private static final String REDIRECTION_LOOP_PONG = "redirection_loop_pong";
    private static final List<String> REDIRECTION_CHAIN = IntStream.range(0, MAX_TABLE_REDIRECTIONS + 1).boxed()
            .map(i -> "redirection_chain_table_" + i)
            .collect(toImmutableList());
    private static final String C0 = "c0";
    private static final String C1 = "c1";
    private static final String C2 = "c2";
    private static final String C3 = "c3";
    private static final String C4 = "c4";

    private static final Map<String, Set<String>> SCHEMA_TABLE_MAPPING = ImmutableMap.of(
            SCHEMA_ONE,
            ImmutableSet.of(TABLE_FOO, VALID_REDIRECTION_SRC, BAD_REDIRECTION_SRC, REDIRECTION_TWICE_SRC, REDIRECTION_LOOP_PING),
            SCHEMA_TWO,
            ImmutableSet.of(TABLE_BAR, VALID_REDIRECTION_TARGET, INTERMEDIATE_TABLE, REDIRECTION_LOOP_PONG),
            SCHEMA_THREE,
            ImmutableSet.copyOf(REDIRECTION_CHAIN));

    private static final Map<SchemaTableName, SchemaTableName> REDIRECTIONS = ImmutableMap.<SchemaTableName, SchemaTableName>builder()
            // Redirection to a valid table
            .put(schemaTableName(SCHEMA_ONE, VALID_REDIRECTION_SRC), schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET))
            // Redirection to a non existent table
            .put(schemaTableName(SCHEMA_ONE, BAD_REDIRECTION_SRC), schemaTableName(SCHEMA_TWO, NON_EXISTENT_TABLE))
            // Multi step redirection
            .put(schemaTableName(SCHEMA_ONE, REDIRECTION_TWICE_SRC), schemaTableName(SCHEMA_TWO, INTERMEDIATE_TABLE))
            .put(schemaTableName(SCHEMA_TWO, INTERMEDIATE_TABLE), schemaTableName(SCHEMA_ONE, TABLE_FOO))
            // Redirection loop
            .put(schemaTableName(SCHEMA_ONE, REDIRECTION_LOOP_PING), schemaTableName(SCHEMA_TWO, REDIRECTION_LOOP_PONG))
            .put(schemaTableName(SCHEMA_TWO, REDIRECTION_LOOP_PONG), schemaTableName(SCHEMA_ONE, REDIRECTION_LOOP_PING))
            // Redirection chain: redirection_chain_table_0 -> redirection_chain_table_1 -> redirection_chain_table_2 ...
            .putAll(IntStream.range(0, REDIRECTION_CHAIN.size() - 1)
                    .boxed()
                    .collect(toImmutableMap(
                            i -> schemaTableName(SCHEMA_THREE, REDIRECTION_CHAIN.get(i)),
                            i -> schemaTableName(SCHEMA_THREE, REDIRECTION_CHAIN.get(i + 1)))))
            .build();

    private static final Map<String, List<ColumnMetadata>> columnMetadatas = ImmutableMap.of(
            SCHEMA_ONE,
            ImmutableList.of(
                    new ColumnMetadata(C0, BIGINT),
                    new ColumnMetadata(C1, BIGINT)),
            SCHEMA_TWO,
            ImmutableList.of(
                    new ColumnMetadata(C2, BIGINT),
                    new ColumnMetadata(C3, BIGINT)),
            SCHEMA_THREE,
            ImmutableList.of(new ColumnMetadata(C4, BIGINT)));

    private static final Function<SchemaTableName, List<ColumnMetadata>> columnsGetter = table -> {
        List<ColumnMetadata> columns = columnMetadatas.get(table.getSchemaName());
        if (columns != null) {
            return columns;
        }
        throw new RuntimeException(format("Unknown schema: %s", table.getSchemaName()));
    };

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(CATALOG_NAME)
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).build();
        queryRunner.installPlugin(new MockConnectorPlugin(createMockConnectorFactory()));
        queryRunner.createCatalog(CATALOG_NAME, "mock", ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory createMockConnectorFactory()
    {
        return MockConnectorFactory.builder()
                .withListSchemaNames(session -> SCHEMAS)
                .withListTables((session, schemaName) -> SCHEMA_TABLE_MAPPING.getOrDefault(schemaName, ImmutableSet.of()).stream()
                        .map(name -> new SchemaTableName(schemaName, name))
                        .collect(toImmutableList()))
                .withStreamTableColumns((session, prefix) -> {
                    List<TableColumnsMetadata> allColumnsMetadata = SCHEMA_TABLE_MAPPING.entrySet().stream()
                            .flatMap(entry -> entry.getValue().stream().map(table -> new SchemaTableName(entry.getKey(), table)))
                            .map(schemaTableName -> {
                                if (REDIRECTIONS.containsKey(schemaTableName)) {
                                    return TableColumnsMetadata.forRedirectedTable(schemaTableName);
                                }
                                return TableColumnsMetadata.forTable(schemaTableName, columnsGetter.apply(schemaTableName));
                            })
                            .collect(toImmutableList());

                    if (prefix.isEmpty()) {
                        return allColumnsMetadata.stream();
                    }

                    String schema = prefix.getSchema().get();

                    if (SCHEMAS.contains(schema)) {
                        return allColumnsMetadata.stream()
                                .filter(columnsMetadata -> columnsMetadata.getTable().getSchemaName().equals(schema))
                                .filter(columnsMetadata -> prefix.getTable().map(columnsMetadata.getTable().getTableName()::equals).orElse(true));
                    }

                    return Stream.empty();
                })
                .withGetTableHandle((session, tableName) -> {
                    if (SCHEMA_TABLE_MAPPING.getOrDefault(tableName.getSchemaName(), ImmutableSet.of()).contains(tableName.getTableName())
                            && !REDIRECTIONS.containsKey(tableName)) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                })
                .withGetViews(((connectorSession, prefix) -> ImmutableMap.of()))
                .withGetColumns(schemaTableName -> {
                    if (!REDIRECTIONS.containsKey(schemaTableName)) {
                        return columnsGetter.apply(schemaTableName);
                    }

                    throw new RuntimeException("Columns do not exist for: " + schemaTableName);
                })
                .withRedirectTable(((connectorSession, schemaTableName) -> {
                    return Optional.ofNullable(REDIRECTIONS.get(schemaTableName))
                            .map(target -> new CatalogSchemaTableName(CATALOG_NAME, target));
                }))
                .build();
    }

    @Test
    public void testTableScans()
    {
        assertQuery(
                format("SELECT c2 FROM %s.%s", SCHEMA_ONE, VALID_REDIRECTION_SRC),
                "SELECT 1 WHERE 1=0",
                verifySingleTableScan(SCHEMA_TWO, VALID_REDIRECTION_TARGET));

        assertThatThrownBy(() -> query((format("SELECT c0 FROM %s.%s", SCHEMA_ONE, BAD_REDIRECTION_SRC))))
                .hasMessageContaining(
                        "Table '%s' redirected to '%s', but the target table '%s' does not exist",
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, BAD_REDIRECTION_SRC),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE));

        assertQuery(
                format("SELECT c0 FROM %s.%s", SCHEMA_ONE, REDIRECTION_TWICE_SRC),
                "SELECT 1 WHERE 1=0",
                verifySingleTableScan(SCHEMA_ONE, TABLE_FOO));

        assertThatThrownBy(() -> query(format("SELECT c0 FROM %s.%s", SCHEMA_ONE, REDIRECTION_LOOP_PING)))
                .hasMessageContaining(
                        "Table redirections form a loop: %s -> %s -> %s",
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, REDIRECTION_LOOP_PING),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, REDIRECTION_LOOP_PONG),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, REDIRECTION_LOOP_PING));

        assertThatThrownBy(() -> query(format("SELECT c4 FROM %s.%s", SCHEMA_THREE, REDIRECTION_CHAIN.get(0))))
                .hasMessageContaining(
                        "Table redirected too many times (10): [%s]",
                        REDIRECTION_CHAIN.stream()
                                .map(table -> new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_THREE, table).toString())
                                .collect(Collectors.joining(", ")));
    }

    @Test
    public void testTableListing()
    {
        assertQuery(
                format("SHOW TABLES FROM %s", SCHEMA_ONE),
                format("VALUES %s",
                        SCHEMA_TABLE_MAPPING.get(SCHEMA_ONE).stream()
                                .map(table -> "('" + table + "')")
                                .collect(Collectors.joining(","))));
        assertQuery(
                format("SELECT table_name FROM system.jdbc.tables WHERE table_cat = '%s' AND table_schem ='%s'", CATALOG_NAME, SCHEMA_ONE),
                format("VALUES %s",
                        SCHEMA_TABLE_MAPPING.get(SCHEMA_ONE).stream()
                                .map(table -> "('" + table + "')")
                                .collect(Collectors.joining(","))));

        assertQuery(
                format("SHOW TABLES FROM %s", SCHEMA_TWO),
                format(
                        "VALUES %s",
                        SCHEMA_TABLE_MAPPING.get(SCHEMA_TWO).stream()
                                .map(table -> "('" + table + "')")
                                .collect(Collectors.joining(","))));
        assertQuery(
                format("SELECT table_name FROM system.jdbc.tables WHERE table_cat = '%s' AND table_schem ='%s'", CATALOG_NAME, SCHEMA_TWO),
                format("VALUES %s",
                        SCHEMA_TABLE_MAPPING.get(SCHEMA_TWO).stream()
                                .map(table -> "('" + table + "')")
                                .collect(Collectors.joining(","))));

        assertQuery(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema != 'information_schema'",
                format(
                        "VALUES %s",
                        SCHEMA_TABLE_MAPPING.entrySet().stream()
                                .map(mappings -> mappings.getValue().stream()
                                        .map(tableName -> row(mappings.getKey(), tableName)))
                                .flatMap(Function.identity())
                                .collect(Collectors.joining(","))));

        assertQuery(
                format("SELECT table_schema, table_name"
                                + " FROM information_schema.tables"
                                + " WHERE table_catalog='%s' AND table_schema = '%s' AND table_name='%s'",
                        CATALOG_NAME,
                        SCHEMA_ONE,
                        VALID_REDIRECTION_SRC),
                format("VALUES ('%s', '%s')", SCHEMA_ONE, VALID_REDIRECTION_SRC));

        assertQueryFails(
                format("SELECT table_schema, table_name"
                                + " FROM information_schema.tables"
                                + " WHERE table_catalog='%s' AND table_schema = '%s' AND table_name='%s'",
                        CATALOG_NAME,
                        SCHEMA_ONE,
                        BAD_REDIRECTION_SRC),
                format("Table '%1$s' redirected to '%2$s', but the target table '%2$s' does not exist",
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, BAD_REDIRECTION_SRC),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE)));

        assertQuery(format(
                "SELECT table_schema, table_name"
                        + " FROM information_schema.tables"
                        + " WHERE table_catalog='%s' AND table_schema = '' AND table_name = ''",
                CATALOG_NAME));
    }

    @Test
    public void testTableColumnsListing()
    {
        String schemaOneColumns = "VALUES "
                + row(SCHEMA_ONE, TABLE_FOO, C0) + ","
                + row(SCHEMA_ONE, TABLE_FOO, C1) + ","
                + row(SCHEMA_ONE, VALID_REDIRECTION_SRC, C2) + ","
                + row(SCHEMA_ONE, VALID_REDIRECTION_SRC, C3) + ","
                + row(SCHEMA_ONE, REDIRECTION_TWICE_SRC, C0) + ","
                + row(SCHEMA_ONE, REDIRECTION_TWICE_SRC, C1);
        assertQuery(format("SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema = '%s'", SCHEMA_ONE), schemaOneColumns);
        assertQuery(format("SELECT table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_schem = '%s' AND table_cat = '%s'", SCHEMA_ONE, CATALOG_NAME), schemaOneColumns);

        String schemaTwoColumns = "VALUES "
                + row(SCHEMA_TWO, TABLE_BAR, C2) + ","
                + row(SCHEMA_TWO, TABLE_BAR, C3) + ","
                + row(SCHEMA_TWO, VALID_REDIRECTION_TARGET, C2) + ","
                + row(SCHEMA_TWO, VALID_REDIRECTION_TARGET, C3) + ","
                + row(SCHEMA_TWO, INTERMEDIATE_TABLE, C0) + ","
                + row(SCHEMA_TWO, INTERMEDIATE_TABLE, C1);
        assertQuery(format("SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema = '%s'", SCHEMA_TWO), schemaTwoColumns);
        assertQuery(format("SELECT table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_schem = '%s' AND table_cat = '%s'", SCHEMA_TWO, CATALOG_NAME), schemaTwoColumns);

        String validRedirectionSrcColumns = "VALUES "
                + row(SCHEMA_ONE, VALID_REDIRECTION_SRC, C2) + ","
                + row(SCHEMA_ONE, VALID_REDIRECTION_SRC, C3);
        assertQuery(format("SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", SCHEMA_ONE, VALID_REDIRECTION_SRC), validRedirectionSrcColumns);
        assertQuery(format("SELECT table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_schem = '%s' AND table_name='%s' AND table_cat = '%s'", SCHEMA_ONE, VALID_REDIRECTION_SRC, CATALOG_NAME), validRedirectionSrcColumns);

        String emptyResult = "SELECT '', '', '' WHERE 1 = 0";
        assertQuery(format("SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", SCHEMA_ONE, BAD_REDIRECTION_SRC), emptyResult);
        assertQuery(format("SELECT table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_schem = '%s' AND table_name='%s' AND table_cat = '%s'", SCHEMA_ONE, BAD_REDIRECTION_SRC, CATALOG_NAME), emptyResult);

        assertQuery(format("SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", SCHEMA_ONE, REDIRECTION_LOOP_PING), emptyResult);
        assertQuery(format("SELECT table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_schem = '%s' AND table_name = '%s' AND table_cat = '%s'", SCHEMA_ONE, REDIRECTION_LOOP_PING, CATALOG_NAME), emptyResult);
    }

    @Test
    public void testShowCreate()
    {
        String showCreateValidSource = (String) computeScalar(format("SHOW CREATE TABLE %s.%s", SCHEMA_ONE, VALID_REDIRECTION_SRC));
        String showCreateValidTarget = (String) computeScalar(format("SHOW CREATE TABLE %s.%s", SCHEMA_TWO, VALID_REDIRECTION_TARGET));
        assertEquals(showCreateValidTarget, showCreateValidSource.replace(SCHEMA_ONE + "." + VALID_REDIRECTION_SRC, SCHEMA_TWO + "." + VALID_REDIRECTION_TARGET));

        assertThatThrownBy(() -> query((format("SHOW CREATE TABLE %s.%s", SCHEMA_ONE, BAD_REDIRECTION_SRC))))
                .hasMessageContaining(
                        "Table '%s' redirected to '%s', but the target table '%s' does not exist",
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, BAD_REDIRECTION_SRC),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE));

        assertThatThrownBy(() -> query((format("SHOW CREATE TABLE %s.%s", SCHEMA_ONE, REDIRECTION_LOOP_PING))))
                .hasMessageContaining("Table redirections form a loop");
    }

    @Test
    public void testDescribeTable()
    {
        assertEquals(computeActual(format("DESCRIBE %s.%s", SCHEMA_ONE, VALID_REDIRECTION_SRC)),
                computeActual(format("DESCRIBE %s.%s", SCHEMA_TWO, VALID_REDIRECTION_TARGET)));

        assertThatThrownBy(() -> query((format("DESCRIBE %s.%s", SCHEMA_ONE, BAD_REDIRECTION_SRC))))
                .hasMessageContaining(
                        "Table '%s' redirected to '%s', but the target table '%s' does not exist",
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, BAD_REDIRECTION_SRC),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE));

        assertThatThrownBy(() -> query((format("DESCRIBE %s.%s", SCHEMA_ONE, REDIRECTION_LOOP_PING))))
                .hasMessageContaining("Table redirections form a loop");
    }

    @Test
    public void testShowColumns()
    {
        assertQuery(
                format("SHOW COLUMNS FROM %s.%s", SCHEMA_ONE, VALID_REDIRECTION_SRC),
                "VALUES "
                        + row(C2, BIGINT.getDisplayName(), "", "") + ","
                        + row(C3, BIGINT.getDisplayName(), "", ""));

        assertThatThrownBy(() -> query((format("SHOW COLUMNS FROM %s.%s", SCHEMA_ONE, BAD_REDIRECTION_SRC))))
                .hasMessageContaining(
                        "Table '%s' redirected to '%s', but the target table '%s' does not exist",
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_ONE, BAD_REDIRECTION_SRC),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE),
                        new CatalogSchemaTableName(CATALOG_NAME, SCHEMA_TWO, NON_EXISTENT_TABLE));

        assertThatThrownBy(() -> query((format("SHOW COLUMNS FROM %s.%s", SCHEMA_ONE, REDIRECTION_LOOP_PING))))
                .hasMessageContaining("Table redirections form a loop");
    }

    @Test
    public void testInsert()
    {
        assertUpdate(
                getSession(),
                format("INSERT INTO %s.%s VALUES (5, 6)", SCHEMA_ONE, VALID_REDIRECTION_SRC),
                1,
                // Verify the insert plan instead of through a successive SELECT, because insertion is a no-op for Mock connector
                plan -> {
                    TableFinishNode finishNode = searchFrom(plan.getRoot())
                            .where(TableFinishNode.class::isInstance)
                            .findOnlyElement();
                    TableWriterNode.InsertTarget insertTarget = ((TableWriterNode.InsertTarget) finishNode.getTarget());
                    assertEquals(
                            ((MockConnectorInsertTableHandle) insertTarget.getHandle().getConnectorHandle()).getTableName(),
                            schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET));
                    assertEquals(insertTarget.getSchemaTableName(), schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET));
                });
    }

    @Test
    public void testDelete()
    {
        assertUpdate(
                getSession(),
                format("DELETE FROM %s.%s WHERE %s = 5", SCHEMA_ONE, VALID_REDIRECTION_SRC, C2),
                0,
                // Verify the insert plan instead of through a successive SELECT, because deletion is a no-op for Mock connector
                plan -> {
                    TableFinishNode finishNode = searchFrom(plan.getRoot())
                            .where(TableFinishNode.class::isInstance)
                            .findOnlyElement();
                    TableWriterNode.DeleteTarget deleteTarget = ((TableWriterNode.DeleteTarget) finishNode.getTarget());
                    assertEquals(
                            ((MockConnectorTableHandle) deleteTarget.getHandle().get().getConnectorHandle()).getTableName(),
                            schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET));
                    assertEquals(deleteTarget.getSchemaTableName(), schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET));
                });
    }

    @Test
    public void testUpdate()
    {
        assertUpdate(
                getSession(),
                format("UPDATE %s.%s SET %s = 5 WHERE %s = 1", SCHEMA_ONE, VALID_REDIRECTION_SRC, C3, C2),
                0,
                // Verify the insert plan instead of through a successive SELECT, because update is a no-op for Mock connector
                plan -> {
                    TableFinishNode finishNode = searchFrom(plan.getRoot())
                            .where(TableFinishNode.class::isInstance)
                            .findOnlyElement();
                    TableWriterNode.UpdateTarget updateTarget = ((TableWriterNode.UpdateTarget) finishNode.getTarget());
                    assertEquals(
                            ((MockConnectorTableHandle) updateTarget.getHandle().get().getConnectorHandle()).getTableName(),
                            schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET));
                    assertEquals(updateTarget.getSchemaTableName(), schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET));
                });
    }

    // TODO: Add tests for redirection in CommentsSystemTable and CREATE TABLE LIKE

    private static String row(String... values)
    {
        return Arrays.stream(values)
                .map(value -> "'" + value + "'")
                .collect(Collectors.joining(",", "(", ")"));
    }

    private Consumer<Plan> verifySingleTableScan(String schemaName, String tableName)
    {
        return plan -> {
            TableScanNode tableScan = searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findOnlyElement();
            SchemaTableName actual = ((MockConnectorTableHandle) tableScan.getTable().getConnectorHandle()).getTableName();
            assertEquals(actual, schemaTableName(schemaName, tableName));
        };
    }
}
