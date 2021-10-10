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
package io.trino.sql.planner.sanity;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.LongLiteral;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValidateLimitWithPresortedInput
        extends BasePlanTest
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final SchemaTableName MOCK_TABLE_NAME = new SchemaTableName(TEST_SCHEMA, "table_a");
    private static final String COLUMN_NAME_A = "col_a";
    private static final ColumnHandle COLUMN_HANDLE_A = new MockConnectorColumnHandle(COLUMN_NAME_A, VARCHAR);
    private static final String COLUMN_NAME_B = "col_b";
    private static final ColumnHandle COLUMN_HANDLE_B = new MockConnectorColumnHandle(COLUMN_NAME_B, VARCHAR);
    private static final String COLUMN_NAME_C = "col_c";
    private static final ColumnHandle COLUMN_HANDLE_C = new MockConnectorColumnHandle(COLUMN_NAME_C, VARCHAR);

    private static final TableHandle MOCK_TABLE_HANDLE = new TableHandle(
            new CatalogName(MOCK_CATALOG),
            new MockConnectorTableHandle(MOCK_TABLE_NAME),
            TestingTransactionHandle.create(),
            Optional.empty());

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(MOCK_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(session).build();
        MockConnectorFactory mockFactory = MockConnectorFactory.builder()
                .withGetTableProperties((connectorSession, handle) -> {
                    MockConnectorTableHandle tableHandle = (MockConnectorTableHandle) handle;
                    if (tableHandle.getTableName().equals(MOCK_TABLE_NAME)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(
                                        new SortingProperty<>(COLUMN_HANDLE_A, ASC_NULLS_FIRST),
                                        new SortingProperty<>(COLUMN_HANDLE_C, ASC_NULLS_FIRST)));
                    }
                    throw new IllegalArgumentException();
                })
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(MOCK_TABLE_NAME)) {
                        return ImmutableList.of(
                                new ColumnMetadata(COLUMN_NAME_A, VARCHAR),
                                new ColumnMetadata(COLUMN_NAME_B, VARCHAR),
                                new ColumnMetadata(COLUMN_NAME_C, VARCHAR));
                    }
                    throw new IllegalArgumentException();
                })
                .build();
        queryRunner.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testValidateSuccessful()
    {
        validatePlan(
                p -> p.limit(
                        10,
                        ImmutableList.of(),
                        true,
                        ImmutableList.of(p.symbol(COLUMN_NAME_A, VARCHAR), p.symbol(COLUMN_NAME_C, VARCHAR)),
                        p.tableScan(
                                MOCK_TABLE_HANDLE,
                                ImmutableList.of(p.symbol(COLUMN_NAME_A, VARCHAR), p.symbol(COLUMN_NAME_B, VARCHAR), p.symbol(COLUMN_NAME_C, VARCHAR)),
                                ImmutableMap.of(
                                        p.symbol(COLUMN_NAME_A, VARCHAR), COLUMN_HANDLE_A,
                                        p.symbol(COLUMN_NAME_B, VARCHAR), COLUMN_HANDLE_B,
                                        p.symbol(COLUMN_NAME_C, VARCHAR), COLUMN_HANDLE_C))));

        validatePlan(
                p -> p.limit(
                        10,
                        ImmutableList.of(),
                        true,
                        ImmutableList.of(p.symbol(COLUMN_NAME_A, VARCHAR)),
                        p.tableScan(
                                MOCK_TABLE_HANDLE,
                                ImmutableList.of(p.symbol(COLUMN_NAME_A, VARCHAR), p.symbol(COLUMN_NAME_B, VARCHAR), p.symbol(COLUMN_NAME_C, VARCHAR)),
                                ImmutableMap.of(
                                        p.symbol(COLUMN_NAME_A, VARCHAR), COLUMN_HANDLE_A,
                                        p.symbol(COLUMN_NAME_B, VARCHAR), COLUMN_HANDLE_B,
                                        p.symbol(COLUMN_NAME_C, VARCHAR), COLUMN_HANDLE_C))));
    }

    @Test
    public void testValidateConstantProperty()
    {
        validatePlan(
                p -> p.limit(
                        10,
                        ImmutableList.of(),
                        true,
                        ImmutableList.of(p.symbol("a", BIGINT)),
                        p.filter(
                                expression("a = 1"),
                                p.values(
                                        ImmutableList.of(p.symbol("a", BIGINT)),
                                        ImmutableList.of(
                                                ImmutableList.of(new LongLiteral("1")),
                                                ImmutableList.of(new LongLiteral("1")))))));
    }

    @Test
    public void testValidateFailed()
    {
        assertThatThrownBy(() -> validatePlan(
                p -> p.limit(
                        10,
                        ImmutableList.of(),
                        true,
                        ImmutableList.of(p.symbol(COLUMN_NAME_B, VARCHAR)),
                        p.tableScan(
                                MOCK_TABLE_HANDLE,
                                ImmutableList.of(p.symbol(COLUMN_NAME_A, VARCHAR), p.symbol(COLUMN_NAME_B, VARCHAR)),
                                ImmutableMap.of(
                                        p.symbol(COLUMN_NAME_A, VARCHAR), COLUMN_HANDLE_A,
                                        p.symbol(COLUMN_NAME_B, VARCHAR), COLUMN_HANDLE_B)))))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("\\QExpected Limit input to be sorted by: [col_b], but was [S↑←(col_a)]\\E");
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        Metadata metadata = queryRunner.getMetadata();
        PlanBuilder builder = new PlanBuilder(idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);
        TypeProvider types = builder.getTypes();

        queryRunner.inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            TypeAnalyzer typeAnalyzer = new TypeAnalyzer(queryRunner.getSqlParser(), metadata);
            new ValidateLimitWithPresortedInput().validate(planNode, session, metadata, queryRunner.getTypeOperators(), typeAnalyzer, types, WarningCollector.NOOP);
            return null;
        });
    }
}
