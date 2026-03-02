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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.spi.StandardErrorCode.MERGE_TARGET_ROW_MULTIPLE_MATCHES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMerge
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction FAIL = FUNCTIONS.resolveFunction("fail", fromTypes(INTEGER, VARCHAR));
    private static final ResolvedFunction NOT = FUNCTIONS.resolveFunction("$not", fromTypes(BOOLEAN));
    private static final Type ROW_TYPE = RowType.anonymousRow(INTEGER, INTEGER, BOOLEAN, TINYINT, INTEGER);

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("schema")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false");

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.installPlugin(
                new MockConnectorPlugin(MockConnectorFactory.builder()
                        .withGetTableHandle((session, schemaTableName) -> {
                            if (schemaTableName.getTableName().equals("test_table_merge_target")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            if (schemaTableName.getTableName().equals("test_table_merge_source")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            return null;
                        })
                        .withGetColumns(name -> ImmutableList.of(
                                new ColumnMetadata("column1", INTEGER),
                                new ColumnMetadata("column2", INTEGER)))
                        .build()));
        planTester.createCatalog("mock", "mock", ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testMergeWithSimpleSelect()
    {
        // one join
        assertPlan(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN MATCHED " +
                        "THEN UPDATE SET column2 = b.column2 " +
                        "WHEN NOT MATCHED " +
                        "THEN INSERT (column1 ,column2) VALUES (b.column1, b.column2)",
                anyTree(
                        filter(
                                new Case(
                                        ImmutableList.of(new WhenClause(new Call(NOT, ImmutableList.of(new Reference(BOOLEAN, "is_distinct"))), new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, (long) MERGE_TARGET_ROW_MULTIPLE_MATCHES.toErrorCode().getCode()), new Constant(VARCHAR, utf8Slice("One MERGE target table row matched more than one source row")))), BOOLEAN))),
                                        TRUE),
                                markDistinct("is_distinct", ImmutableList.of("unique_id", "case_number"),
                                        anyTree(
                                                project(ImmutableMap.of(
                                                        "unique_id", expression(new Coalesce(ImmutableList.of(new Reference(BIGINT, "target_unique_id"), new Reference(BIGINT, "source_unique_id")))),
                                                        "field", expression(new Reference(BIGINT, "field")),
                                                        "merge_row", expression(new Reference(ROW_TYPE, "merge_row")),
                                                        "case_number", expression(new FieldReference(new Reference(ROW_TYPE, "merge_row"), 4))),
                                                        project(ImmutableMap.of(
                                                                "field", expression(new Reference(BIGINT, "field")),
                                                                "merge_row", expression(new Case(
                                                                        ImmutableList.of(
                                                                                new WhenClause(new Reference(BOOLEAN, "present"), new Row(ImmutableList.of(new Reference(INTEGER, "column1"), new Reference(INTEGER, "column2_1"), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Constant(TINYINT, 3L), new Constant(INTEGER, 0L)), ROW_TYPE)),
                                                                                new WhenClause(new IsNull(new Reference(BOOLEAN, "present")), new Row(ImmutableList.of(new Reference(INTEGER, "column1_0"), new Reference(INTEGER, "column2_1"), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Constant(TINYINT, 1L), new Constant(INTEGER, 1L)), ROW_TYPE))),
                                                                        new Constant(ROW_TYPE, null))),
                                                                "target_unique_id", expression(new Reference(BIGINT, "target_unique_id")),
                                                                "source_unique_id", expression(new Reference(BIGINT, "source_unique_id"))),
                                                                            join(RIGHT, builder -> builder
                                                                                    .equiCriteria("column1", "column1_0")
                                                                                    .left(
                                                                                            project(ImmutableMap.of(
                                                                                                            "column1", expression(new Reference(INTEGER, "column1")),
                                                                                                            "field", expression(new Reference(BIGINT, "field")),
                                                                                                            "target_unique_id", expression(new Reference(BIGINT, "target_unique_id")),
                                                                                                            "present", expression(new Constant(BOOLEAN, true))),
                                                                                                    assignUniqueId("target_unique_id",
                                                                                                            tableScan(
                                                                                                                    tableHandle -> ((MockConnectorTableHandle) tableHandle).getTableName().getTableName().equals("test_table_merge_target"),
                                                                                                                    TupleDomain.all(),
                                                                                                                    ImmutableMap.of(
                                                                                                                            "column1", columnHandle -> ((MockConnectorColumnHandle) columnHandle).name().equals("column1"),
                                                                                                                            "field", columnHandle -> ((MockConnectorColumnHandle) columnHandle).name().equals("merge_row_id"))))))
                                                                                    .right(
                                                                                            anyTree(
                                                                                                    assignUniqueId("source_unique_id",
                                                                                                            tableScan("test_table_merge_source", ImmutableMap.of("column1_0", "column1", "column2_1", "column2")))))))))))));
    }
}
