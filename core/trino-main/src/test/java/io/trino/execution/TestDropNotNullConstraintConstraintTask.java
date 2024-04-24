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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.tree.DropNotNullConstraint;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDropNotNullConstraintConstraintTask
        extends BaseDataDefinitionTaskTest
{
    @Override
    @BeforeEach
    public void setUp()
    {
        super.setUp();
        metadata = new MockMetadataWithNotNull(TEST_CATALOG_NAME);
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();
    }

    @Test
    public void testDropNotNullConstraint()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");

        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(notNullColumn("a", BIGINT), notNullColumn("b", BIGINT));

        getFutureValue(executeDropNotNullConstraint(asQualifiedName(tableName), identifier("b"), false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(notNullColumn("a", BIGINT), nullableColumn("b", BIGINT));
    }

    @Test
    public void testDropNotNullConstraintNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropNotNullConstraint(asQualifiedName(tableName), identifier("b"), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testDropNotNullConstraintNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeDropNotNullConstraint(tableName, identifier("b"), true));
        // no exception
    }

    @Test
    public void testDropNotNullConstraintMissingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropNotNullConstraint(asQualifiedName(tableName), identifier("missing_column"), false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column 'missing_column' does not exist");
    }

    @Test
    public void testDropNotNullConstraintNullableColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(nullableColumn("a", BIGINT)));
        metadata.createTable(testSession, TEST_CATALOG_NAME, tableMetadata, FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropNotNullConstraint(asQualifiedName(tableName), identifier("a"), false)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Column is already nullable");
    }

    @Test
    public void testDropNotNullConstraintOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropNotNullConstraint(asQualifiedName(viewName), identifier("test"), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a view with that name exists.", viewName);
    }

    @Test
    public void testDropNotNullConstraintOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropNotNullConstraint(asQualifiedName(materializedViewName), identifier("test"), false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists.", materializedViewName);
    }

    private ListenableFuture<Void> executeDropNotNullConstraint(QualifiedName table, Identifier column, boolean tableExists)
    {
        return new DropNotNullConstraintTask(plannerContext.getMetadata(), new AllowAllAccessControl())
                .execute(new DropNotNullConstraint(new NodeLocation(1, 1), table, column, tableExists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private static ConnectorTableMetadata simpleTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(notNullColumn("a", BIGINT), notNullColumn("b", BIGINT)));
    }

    private static ColumnMetadata nullableColumn(String name, Type type)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(true)
                .build();
    }

    private static ColumnMetadata notNullColumn(String name, Type type)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(false)
                .build();
    }

    private static class MockMetadataWithNotNull
            extends MockMetadata
    {
        public MockMetadataWithNotNull(String catalogName)
        {
            super(catalogName);
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
        {
            return ImmutableSet.of(NOT_NULL_COLUMN_CONSTRAINT);
        }
    }
}
