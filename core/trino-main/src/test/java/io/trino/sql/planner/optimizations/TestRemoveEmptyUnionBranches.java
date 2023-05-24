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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;

public class TestRemoveEmptyUnionBranches
        extends BasePlanTest
{
    private static final String CATALOG_NAME = "test";
    private static final String SCHEMA_NAME = "default";

    private final Set<String> tables = ImmutableSet.of("table_one", "table_two", "table_three");
    private final List<String> columnNames = ImmutableList.of("a", "b", "c");
    private final String pushdownColumn = "c";
    private final Map<String, ColumnHandle> columnHandles = columnNames.stream()
            .collect(toImmutableMap(Function.identity(), name -> new MockConnectorColumnHandle(name, VARCHAR)));

    private final Map<SchemaTableName, ConnectorViewDefinition> views =
            ImmutableMap.of(
                    new SchemaTableName(SCHEMA_NAME, "view_of_union"),
                    new ConnectorViewDefinition(
                            "SELECT " +
                                    "   t1.a, t1.b, t1.c " +
                                    "FROM " +
                                    "   table_one t1 " +
                                    "WHERE " +
                                    "   t1.c = 'X' " +
                                    "UNION ALL " +
                                    "SELECT " +
                                    "   t2.a, t2.b, t2.c " +
                                    "FROM " +
                                    "   table_two t2 " +
                                    "WHERE " +
                                    "   t2.c = 'Y'",
                            Optional.of(CATALOG_NAME),
                            Optional.of(SCHEMA_NAME),
                            columnNames.stream()
                                    .map(name -> new ConnectorViewDefinition.ViewColumn(name, VARCHAR.getTypeId(), Optional.empty()))
                                    .collect(toImmutableList()),
                            Optional.empty(),
                            Optional.empty(),
                            true));

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(
                testSessionBuilder()
                        .setCatalog(CATALOG_NAME)
                        .setSchema(SCHEMA_NAME)
                        .build());
        queryRunner.createCatalog(
                CATALOG_NAME,
                createConnectorFactory(CATALOG_NAME),
                ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory createConnectorFactory(String catalogHandle)
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle((session, tableName) -> {
                    if (tableName.getSchemaName().equals(SCHEMA_NAME) && tables.contains(tableName.getTableName())) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                })
                .withGetViews((session, schemaName) -> {
                    return views;
                })
                .withGetColumns(schemaTableName -> columnNames.stream()
                        .map(name -> new ColumnMetadata(name, VARCHAR))
                        .collect(toImmutableList()))
                .withGetTableProperties((session, handle) -> {
                    MockConnectorTableHandle table = (MockConnectorTableHandle) handle;
                    return new ConnectorTableProperties(table.getConstraint(), Optional.empty(), Optional.empty(), emptyList());
                })
                .withApplyFilter(applyFilter())
                .withName(catalogHandle)
                .build();
    }

    private MockConnectorFactory.ApplyFilter applyFilter()
    {
        return (session, table, constraint) -> {
            if (table instanceof MockConnectorTableHandle handle) {
                SchemaTableName schemaTable = handle.getTableName();
                if (schemaTable.getSchemaName().equals(SCHEMA_NAME) && tables.contains(schemaTable.getTableName())) {
                    Predicate<ColumnHandle> shouldPushdown = columnHandle -> ((MockConnectorColumnHandle) columnHandle).getName().equals(pushdownColumn);
                    TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
                    TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary()
                            .filter((columnHandle, domain) -> shouldPushdown.test(columnHandle)));

                    // Check if predicate and constraint lead to Tupledomain.none().
                    boolean nonePredicateOnPushdownColumn = discoveredNonePredicateOnPushdownColumn(newDomain, constraint);
                    if (nonePredicateOnPushdownColumn) {
                        return Optional.of(
                                new ConstraintApplicationResult<>(
                                        new MockConnectorTableHandle(handle.getTableName(), TupleDomain.none(), Optional.empty()),
                                        constraint.getSummary()
                                                .filter((ch, domain) -> !shouldPushdown.test(ch)),
                                        false));
                    }

                    if (oldDomain.equals(newDomain)) {
                        return Optional.empty();
                    }

                    return Optional.of(
                            new ConstraintApplicationResult<>(
                                    new MockConnectorTableHandle(handle.getTableName(), newDomain, Optional.empty()),
                                    constraint.getSummary()
                                            .filter((columnHandle, domain) -> !shouldPushdown.test(columnHandle)),
                                    false));
                }
            }

            return Optional.empty();
        };
    }

    // This method tries to detect whether we encountered a domain and a predicate that yields no values - and hence
    // results in a "none" domain effectively.
    private boolean discoveredNonePredicateOnPushdownColumn(TupleDomain<ColumnHandle> domain, Constraint constraint)
    {
        if (domain.isNone() || constraint.predicate().isEmpty()) {
            // We're not discovering a new "none" domain if the domain is already none OR
            // predicate isn't present
            return false;
        }

        Domain pushdownColumnDomain = domain.getDomains().get().get(columnHandles.get(pushdownColumn));
        Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate = constraint.predicate();

        if (pushdownColumnDomain != null && pushdownColumnDomain.isNullableDiscreteSet()) {
            Domain.DiscreteSet discreteSet = pushdownColumnDomain.getNullableDiscreteSet();
            if (discreteSet != null) {
                List<NullableValue> nullableValues = discreteSet.getNonNullValues().stream()
                        .map(object -> NullableValue.of(VARCHAR, object))
                        .collect(toImmutableList());
                ColumnHandle columnHandle = new MockConnectorColumnHandle("c", VARCHAR);

                return nullableValues.stream()
                        .allMatch(value -> !predicate.get().test(ImmutableMap.of(columnHandle, value)));
            }
        }

        return false;
    }

    @Test
    public void testRemoveUnionBranches()
    {
        assertPlan("SELECT v1.a FROM view_of_union v1 JOIN table_three t3 ON v1.a = t3.a WHERE substring(v1.c, 1, 10) = 'Y' ",
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("symbol_a", "symbol_a2")
                                .left(anyTree(tableScan("table_two", ImmutableMap.of("symbol_a", "a", "symbol_c", "c"))))
                                .right(anyTree(tableScan("table_three", ImmutableMap.of("symbol_a2", "a"))))
                                .build())));
    }
}
