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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.client.ClientCapabilities;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.informationschema.InformationSchemaColumnHandle;
import io.trino.connector.informationschema.InformationSchemaMetadata;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.stream;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestInformationSchemaMetadata
{
    private LocalQueryRunner queryRunner;
    private TransactionManager transactionManager;
    private Metadata metadata;

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withListTables((connectorSession, schemaName) ->
                        ImmutableList.of("test_view", "another_table"))
                .withGetViews((connectorSession, prefix) -> {
                    ConnectorViewDefinition definition = new ConnectorViewDefinition(
                            "select 1",
                            Optional.of("test_catalog"),
                            Optional.of("test_schema"),
                            ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId(), Optional.of("test column comment"))),
                            Optional.of("comment"),
                            Optional.empty(),
                            true);
                    SchemaTableName viewName = new SchemaTableName("test_schema", "test_view");
                    return ImmutableMap.of(viewName, definition);
                })
                .build();
        queryRunner.createCatalog("test_catalog", mockConnectorFactory, ImmutableMap.of());
        transactionManager = queryRunner.getTransactionManager();
        metadata = queryRunner.getMetadata();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        try {
            if (queryRunner != null) {
                queryRunner.close();
            }
        }
        finally {
            metadata = null;
            transactionManager = null;
            queryRunner = null;
        }
    }

    /**
     * Tests information schema predicate pushdown when both schema and table name are specified.
     */
    @Test
    public void testInformationSchemaPredicatePushdown()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);

        ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builder();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        domains.put(new InformationSchemaColumnHandle("table_name"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_view")));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(domains.buildOrThrow()));

        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle)
                metadata.getTableHandle(session, new SchemaTableName("information_schema", "views"));
        tableHandle = metadata.applyFilter(session, tableHandle, constraint)
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);
        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "test_schema", "test_view")));
    }

    @Test
    public void testInformationSchemaPredicatePushdownWithConstraintPredicate()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Constraint constraint = new Constraint(TupleDomain.all(), TestInformationSchemaMetadata::testConstraint, testConstraintColumns());

        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle)
                metadata.getTableHandle(session, new SchemaTableName("information_schema", "columns"));
        tableHandle = metadata.applyFilter(session, tableHandle, constraint)
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);

        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "test_schema", "test_view")));
    }

    @Test
    public void testInformationSchemaPredicatePushdownWithoutSchemaPredicate()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);

        // predicate without schema predicates should cause schemas to be enumerated when table predicates are present
        ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builder();
        domains.put(new InformationSchemaColumnHandle("table_name"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_view")));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(domains.buildOrThrow()));

        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle)
                metadata.getTableHandle(session, new SchemaTableName("information_schema", "views"));
        tableHandle = metadata.applyFilter(session, tableHandle, constraint)
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);
        // filter blindly applies filter to all visible schemas, so information_schema must be included
        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(
                new QualifiedTablePrefix("test_catalog", "test_schema", "test_view"),
                new QualifiedTablePrefix("test_catalog", "information_schema", "test_view")));
    }

    @Test
    public void testInformationSchemaPredicatePushdownWithoutTablePredicate()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);

        // predicate without table name predicates should not cause table level prefixes to be evaluated
        ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builder();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(domains.buildOrThrow()));

        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle)
                metadata.getTableHandle(session, new SchemaTableName("information_schema", "views"));
        tableHandle = metadata.applyFilter(session, tableHandle, constraint)
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);
        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "test_schema")));
    }

    @Test
    public void testInformationSchemaPredicatePushdownWithConstraintPredicateOnViewsTable()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);

        // predicate on non columns enumerating table should not cause tables to be enumerated
        Constraint constraint = new Constraint(TupleDomain.all(), TestInformationSchemaMetadata::testConstraint, testConstraintColumns());
        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle)
                metadata.getTableHandle(session, new SchemaTableName("information_schema", "views"));
        tableHandle = metadata.applyFilter(session, tableHandle, constraint)
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);

        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "test_schema")));
    }

    @Test
    public void testInformationSchemaPredicatePushdownOnCatalogWiseTables()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);

        // Predicate pushdown shouldn't work for catalog-wise tables because the table prefixes for them are always
        // ImmutableSet.of(new QualifiedTablePrefix(catalogName));
        Constraint constraint = new Constraint(TupleDomain.all());
        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle)
                metadata.getTableHandle(session, new SchemaTableName("information_schema", "schemata"));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(session, tableHandle, constraint);
        assertFalse(result.isPresent());
    }

    @Test
    public void testInformationSchemaPredicatePushdownForEmptyNames()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);
        ConnectorSession session = createNewSession(transactionId);
        ConnectorMetadata metadata = new InformationSchemaMetadata("test_catalog", this.metadata);
        InformationSchemaColumnHandle tableSchemaColumn = new InformationSchemaColumnHandle("table_schema");
        InformationSchemaColumnHandle tableNameColumn = new InformationSchemaColumnHandle("table_name");
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, new SchemaTableName("information_schema", "tables"));

        // Empty schema name
        InformationSchemaTableHandle filtered = metadata.applyFilter(session, tableHandle, new Constraint(TupleDomain.withColumnDomains(
                        ImmutableMap.of(tableSchemaColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice(""))))))
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);

        // "" schema name is valid schema name, but is (currently) valid for QualifiedTablePrefix
        assertEquals(filtered.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "")));

        // Empty table name
        filtered = metadata.applyFilter(session, tableHandle, new Constraint(TupleDomain.withColumnDomains(
                        ImmutableMap.of(tableNameColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice(""))))))
                .map(ConstraintApplicationResult::getHandle)
                .map(InformationSchemaTableHandle.class::cast)
                .orElseThrow(AssertionError::new);

        // "" table name is valid schema name, but is (currently) valid for QualifiedTablePrefix
        // filter blindly applies filter to all visible schemas, so information_schema must be included
        assertEquals(filtered.getPrefixes(), ImmutableSet.of(
                new QualifiedTablePrefix("test_catalog", "test_schema", ""),
                new QualifiedTablePrefix("test_catalog", "information_schema", "")));
    }

    /**
     * @see #testConstraintColumns()
     */
    private static boolean testConstraint(Map<ColumnHandle, NullableValue> bindings)
    {
        // test_schema has a table named "another_table" and we filter that out in this predicate

        // Note: the columns inspected here must be in sync with testConstraintColumns()
        NullableValue catalog = bindings.get(new InformationSchemaColumnHandle("table_catalog"));
        NullableValue schema = bindings.get(new InformationSchemaColumnHandle("table_schema"));
        NullableValue table = bindings.get(new InformationSchemaColumnHandle("table_name"));

        boolean isValid = true;
        if (catalog != null) {
            isValid = ((Slice) catalog.getValue()).toStringUtf8().equals("test_catalog");
        }
        if (schema != null) {
            isValid &= ((Slice) schema.getValue()).toStringUtf8().equals("test_schema");
        }
        if (table != null) {
            isValid &= ((Slice) table.getValue()).toStringUtf8().equals("test_view");
        }
        return isValid;
    }

    /**
     * Returns set of columns inspected by {@link #testConstraint(Map)}.
     */
    private static Set<ColumnHandle> testConstraintColumns()
    {
        return Set.of(
                new InformationSchemaColumnHandle("table_catalog"),
                new InformationSchemaColumnHandle("table_schema"),
                new InformationSchemaColumnHandle("table_name"));
    }

    private static ConnectorSession createNewSession(TransactionId transactionId)
    {
        return testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("information_schema")
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .setTransactionId(transactionId)
                .build()
                .toConnectorSession();
    }
}
