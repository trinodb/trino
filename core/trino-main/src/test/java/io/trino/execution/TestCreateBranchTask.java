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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.BranchPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.tree.CreateBranch;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SaveMode;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.BRANCH_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_BRANCH_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCreateBranchTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    void testCreateBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        assertBranches(tableName, "main");

        metadata.createBranch(testSession, table, "dev", Optional.empty(), FAIL, Map.of());
        assertBranches(tableName, "main", "dev");
    }

    @Test
    void testReplaceBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.REPLACE, "main", Optional.empty(), List.of()));
        assertBranches(tableName, "main");
    }

    @Test
    void testCreateBranchIfNotExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.IGNORE, "main", Optional.empty(), List.of()));
        assertBranches(tableName, "main");
    }

    @Test
    void testCreateBranchFromOtherBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        metadata.createBranch(testSession, table, "tmp", Optional.empty(), FAIL, Map.of());
        assertBranches(tableName, "main", "tmp");

        metadata.createBranch(testSession, table, "dev", Optional.of("tmp"), FAIL, Map.of());
        assertBranches(tableName, "main", "tmp", "dev");
    }

    @Test
    void testCreateBranchFromNonExistentBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.FAIL, "dev", Optional.of("non-existent"), List.of())))
                .hasErrorCode(BRANCH_NOT_FOUND)
                .hasMessage("line 1:1: Branch 'non-existent' does not exist");
    }

    @Test
    void testCreateBranchWithUnknownProperties()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        Property unknownProperty = new Property(new NodeLocation(1, 1), new Identifier("unknown_property"), new StringLiteral(new NodeLocation(1, 1), "unknown"));
        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.FAIL, "dev", Optional.empty(), List.of(unknownProperty))))
                .hasErrorCode(INVALID_BRANCH_PROPERTY)
                .hasMessage("line 1:1: Catalog 'test_catalog' branch property 'unknown_property' does not exist");
        assertBranches(tableName, "main");
    }

    @Test
    void testCreateBranchWithInvalidProperties()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        Property unknownProperty = new Property(new NodeLocation(1, 1), new Identifier("boolean_property"), new StringLiteral(new NodeLocation(1, 1), "unknown"));
        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.FAIL, "dev", Optional.empty(), List.of(unknownProperty))))
                .hasErrorCode(INVALID_BRANCH_PROPERTY)
                .hasMessage("line 1:1: Invalid value for catalog 'test_catalog' branch property 'boolean_property': Cannot convert ['unknown'] to boolean");
        assertBranches(tableName, "main");
    }

    @Test
    void testCreateDuplicateBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.FAIL, "main", Optional.empty(), List.of())))
                .hasErrorCode(BRANCH_ALREADY_EXISTS)
                .hasMessage("line 1:1: Branch 'main' already exists");
        assertBranches(tableName, "main");
    }

    @Test
    void testCreateBranchOnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(asQualifiedName(tableName), SaveMode.FAIL, "not_found", Optional.empty(), List.of())))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("line 1:1: Table 'test_catalog.schema.existing_table' does not exist");
    }

    @Test
    void testCreateBranchOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(asQualifiedName(viewName), SaveMode.FAIL, "main", Optional.empty(), List.of())))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Creating branch from view is not supported");
    }

    @Test
    void testCreateBranchOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateBranch(viewName, SaveMode.FAIL, "main", Optional.empty(), List.of())))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Creating branch from materialized view is not supported");
    }

    private ListenableFuture<Void> executeCreateBranch(QualifiedName tableName, SaveMode saveMode, String branchName, Optional<String> fromBranch, List<Property> properties)
    {
        CreateBranch createBranch = new CreateBranch(new NodeLocation(1, 1), tableName, new Identifier(branchName), fromBranch.map(Identifier::new), saveMode, properties);
        BranchPropertyManager propertyManager = new BranchPropertyManager(_ -> ImmutableMap.of("boolean_property", booleanProperty("boolean_property", "Mock description", false, false)));
        return new CreateBranchTask(plannerContext, metadata, new AllowAllAccessControl(), propertyManager)
                .execute(createBranch, queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private void assertBranches(QualifiedObjectName tableName, String... expectedBranches)
    {
        assertThat(metadata.listBranches(testSession, tableName))
                .containsExactlyInAnyOrder(expectedBranches);
    }
}
