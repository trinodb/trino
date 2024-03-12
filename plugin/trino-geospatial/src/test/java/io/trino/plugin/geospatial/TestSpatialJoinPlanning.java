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
package io.trino.plugin.geospatial;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.trino.Session;
import io.trino.block.BlockSerdeUtil;
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.plugin.memory.MemoryConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.SystemSessionProperties.SPATIAL_PARTITIONING_TABLE_NAME;
import static io.trino.geospatial.KdbTree.Node.newLeaf;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSpatialJoinPlanning
        extends BasePlanTest
{
    private static final String KDB_TREE_JSON = KdbTreeUtils.toJson(new KdbTree(newLeaf(new Rectangle(0, 0, 10, 10), 0)));

    private Expression kdbTreeLiteral;

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build());
        planTester.installPlugin(new GeoPlugin());
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        planTester.createCatalog("memory", new MemoryConnectorFactory(), ImmutableMap.of());
        planTester.executeStatement(format("CREATE TABLE kdb_tree AS SELECT '%s' AS v", KDB_TREE_JSON));
        planTester.executeStatement("CREATE TABLE points (lng, lat, name) AS (VALUES (2.1e0, 2.1e0, 'x'))");
        planTester.executeStatement("CREATE TABLE polygons (wkt, name) AS (VALUES ('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))', 'a'))");
        return planTester;
    }

    @BeforeAll
    public void setUp()
    {
        Block block = nativeValueToBlock(KdbTreeType.KDB_TREE, KdbTreeUtils.fromJson(KDB_TREE_JSON));
        DynamicSliceOutput output = new DynamicSliceOutput(0);
        BlockSerdeUtil.writeBlock(new TestingBlockEncodingSerde(), output, block);
        kdbTreeLiteral = new FunctionCall(QualifiedName.of("$literal$"), ImmutableList.of(
                new FunctionCall(QualifiedName.of("from_base64"), ImmutableList.of(
                        new StringLiteral(Base64.getEncoder().encodeToString(output.slice().getBytes()))))));
    }

    @Test
    public void testSpatialJoinContains()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                project(ImmutableMap.of(
                                                "st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat")))),
                                                "length", expression(new FunctionCall(QualifiedName.of("length"), ImmutableList.of(new SymbolReference("name"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of(
                                                        "st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar"))))),
                                                        "length_2", expression(new FunctionCall(QualifiedName.of("length"), ImmutableList.of(new SymbolReference("name_2"))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                Optional.of(KDB_TREE_JSON),
                                Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_a", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("st_point"))))),
                                                        project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_b", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("st_geometryfromtext"))))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))))));
    }

    @Test
    public void testSpatialJoinWithin()
    {
        // broadcast
        assertPlan("SELECT points.name, polygons.name " +
                        "FROM points, polygons " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_within"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(new FunctionCall(QualifiedName.of("st_within"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                project(ImmutableMap.of(
                                                "st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat")))),
                                                "length", expression(new FunctionCall(QualifiedName.of("length"), ImmutableList.of(new SymbolReference("name"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of(
                                                        "st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar"))))),
                                                        "length_2", expression(new FunctionCall(QualifiedName.of("length"), ImmutableList.of(new SymbolReference("name_2"))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_within"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                Optional.of(KDB_TREE_JSON),
                                Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_a", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("st_point"))))),
                                                        project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_b", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("st_geometryfromtext"))))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))))));
    }

    @Test
    public void testInvalidKdbTree()
    {
        // table doesn't exist
        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("non_existent_table"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Table not found: memory.default.non_existent_table");

        // empty table
        getPlanTester().executeStatement("CREATE TABLE empty_table AS SELECT 'a' AS v WHERE false");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("empty_table"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected exactly one row for table memory.default.empty_table, but got none");

        // invalid JSON
        getPlanTester().executeStatement("CREATE TABLE invalid_kdb_tree AS SELECT 'invalid-json' AS v");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("invalid_kdb_tree"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Invalid JSON string for KDB tree: .*");

        // more than one row
        getPlanTester().executeStatement(format("CREATE TABLE too_many_rows AS SELECT * FROM (VALUES '%s', '%s') AS t(v)", KDB_TREE_JSON, KDB_TREE_JSON));

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("too_many_rows"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected exactly one row for table memory.default.too_many_rows, but found 2 rows");

        // more than one column
        getPlanTester().executeStatement("CREATE TABLE too_many_columns AS SELECT '%s' as c1, 100 as c2");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("too_many_columns"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected single column for table memory.default.too_many_columns, but found 2 columns");
    }

    private void assertInvalidSpatialPartitioning(Session session, String sql, String expectedMessageRegExp)
    {
        PlanTester planTester = getPlanTester();
        try {
            planTester.inTransaction(session, transactionSession -> planTester.createPlan(transactionSession, sql));
            throw new AssertionError(format("Expected query to fail: %s", sql));
        }
        catch (TrinoException ex) {
            assertThat(ex.getErrorCode()).isEqualTo(INVALID_SPATIAL_PARTITIONING.toErrorCode());
            if (!nullToEmpty(ex.getMessage()).matches(expectedMessageRegExp)) {
                throw new AssertionError(format("Expected exception message '%s' to match '%s' for query: %s", ex.getMessage(), expectedMessageRegExp, sql), ex);
            }
        }
    }

    @Test
    public void testSpatialJoinIntersects()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_intersects"), ImmutableList.of(new SymbolReference("geometry_a"), new SymbolReference("geometry_b"))),
                                project(ImmutableMap.of("geometry_a", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt_a"), dataType("varchar")))))),
                                        tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("geometry_b", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt_b"), dataType("varchar")))))),
                                                tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                withSpatialPartitioning("default.kdb_tree"),
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_intersects"), ImmutableList.of(new SymbolReference("geometry_a"), new SymbolReference("geometry_b"))), Optional.of(KDB_TREE_JSON), Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_a", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("geometry_a"))))),
                                                        project(ImmutableMap.of("geometry_a", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt_a"), dataType("varchar")))))),
                                                                tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name")))))),
                                anyTree(
                                        project(ImmutableMap.of("partitions_b", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("geometry_b"))))),
                                                project(ImmutableMap.of("geometry_b", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt_b"), dataType("varchar")))))),
                                                        tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name"))))))));
    }

    @Test
    public void testNotContains()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE NOT ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        filter(
                                new NotExpression(new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))), new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat")))))),
                                join(INNER, builder -> builder
                                        .left(tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name")))
                                        .right(
                                                anyTree(
                                                        tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name"))))))));
    }

    @Test
    public void testNotIntersects()
    {
        assertPlan(format("SELECT b.name, a.name " +
                        "FROM " +
                        singleRow("IF(rand() >= 0, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')", "'a'") + " AS a (wkt, name), " +
                        singleRow("IF(rand() >= 0, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')", "'a'") + " AS b (wkt, name) " +
                        "           WHERE NOT ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))", singleRow()),
                anyTree(
                        filter(
                                new NotExpression(
                                        functionCall("ST_Intersects", ImmutableList.of(GEOMETRY, GEOMETRY), ImmutableList.of(
                                                functionCall("ST_GeometryFromText", ImmutableList.of(VARCHAR), ImmutableList.of(new Cast(new SymbolReference("wkt_a"), dataType("varchar")))),
                                                functionCall("ST_GeometryFromText", ImmutableList.of(VARCHAR), ImmutableList.of(new Cast(new SymbolReference("wkt_b"), dataType("varchar"))))))),
                                join(INNER, builder -> builder
                                        .left(
                                                project(
                                                        ImmutableMap.of(
                                                                "wkt_a", expression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("random"), ImmutableList.of()), new DoubleLiteral("0.0")), new StringLiteral("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))), Optional.empty())),
                                                                "name_a", expression(new StringLiteral("a"))),
                                                        singleRow()))
                                        .right(
                                                any(project(
                                                        ImmutableMap.of(
                                                                "wkt_b", expression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("random"), ImmutableList.of()), new DoubleLiteral("0.0")), new StringLiteral("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))), Optional.empty())),
                                                                "name_b", expression(new StringLiteral("a"))),
                                                        singleRow())))))));
    }

    @Test
    public void testContainsWithEquiClause()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE a.name = b.name AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("name_a", "name_b")
                                .filter(new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))), new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))))
                                .left(
                                        anyTree(
                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))))
                                .right(
                                        anyTree(
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));
    }

    @Test
    public void testIntersectsWithEquiClause()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE a.name = b.name AND ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("name_a", "name_b")
                                .filter(new FunctionCall(QualifiedName.of("st_intersects"), ImmutableList.of(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt_a"), dataType("varchar")))), new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt_b"), dataType("varchar")))))))
                                .left(
                                        anyTree(
                                                tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name"))))
                                .right(
                                        anyTree(
                                                tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name")))))));
    }

    @Test
    public void testSpatialLeftJoins()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialLeftJoin(
                                new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND a.name <> b.name",
                anyTree(
                        spatialLeftJoin(
                                new LogicalExpression(AND, ImmutableList.of(new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))), new ComparisonExpression(NOT_EQUAL, new SymbolReference("name_a"), new SymbolReference("name_b")))),
                                project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // non-deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND rand() < 0.5",
                anyTree(
                        spatialLeftJoin(
                                new LogicalExpression(AND, ImmutableList.of(new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))), new ComparisonExpression(LESS_THAN, new FunctionCall(QualifiedName.of("random"), ImmutableList.of()), new DoubleLiteral("0.5")))),
                                project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // filter over join
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "   ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) " +
                        "WHERE concat(a.name, b.name) is null",
                anyTree(
                        filter(
                                new IsNullPredicate(new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(new Cast(new SymbolReference("name_a"), dataType("varchar")), new Cast(new SymbolReference("name_b"), dataType("varchar"))))),
                                spatialLeftJoin(
                                        new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("st_geometryfromtext"), new SymbolReference("st_point"))),
                                        project(ImmutableMap.of("st_point", expression(new FunctionCall(QualifiedName.of("st_point"), ImmutableList.of(new SymbolReference("lng"), new SymbolReference("lat"))))),
                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                        anyTree(
                                                project(ImmutableMap.of("st_geometryfromtext", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("wkt"), dataType("varchar")))))),
                                                        tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name"))))))));
    }

    @Test
    public void testDistributedSpatialJoinOverUnion()
    {
        // union on the left side
        assertDistributedPlan("SELECT a.name, b.name " +
                        "FROM (SELECT name FROM tpch.tiny.region UNION ALL SELECT name FROM tpch.tiny.nation) a, tpch.tiny.customer b " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.name), ST_GeometryFromText(b.name))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("g1"), new SymbolReference("g3"))), Optional.of(KDB_TREE_JSON), Optional.empty(),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p1", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("g1"))))),
                                                        project(ImmutableMap.of("g1", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("name_a1"), dataType("varchar")))))),
                                                                tableScan("region", ImmutableMap.of("name_a1", "name")))),
                                                project(ImmutableMap.of("p2", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("g2"))))),
                                                        project(ImmutableMap.of("g2", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("name_a2"), dataType("varchar")))))),
                                                                tableScan("nation", ImmutableMap.of("name_a2", "name"))))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p3", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("g3"))))),
                                                        project(ImmutableMap.of("g3", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("name_b"), dataType("varchar")))))),
                                                                tableScan("customer", ImmutableMap.of("name_b", "name")))))))));

        // union on the right side
        assertDistributedPlan("SELECT a.name, b.name " +
                        "FROM tpch.tiny.customer a, (SELECT name FROM tpch.tiny.region UNION ALL SELECT name FROM tpch.tiny.nation) b " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.name), ST_GeometryFromText(b.name))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(
                                new FunctionCall(QualifiedName.of("st_contains"), ImmutableList.of(new SymbolReference("g1"), new SymbolReference("g2"))),
                                Optional.of(KDB_TREE_JSON),
                                Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p1", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("g1"))))),
                                                        project(ImmutableMap.of("g1", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("name_a"), dataType("varchar")))))),
                                                                tableScan("customer", ImmutableMap.of("name_a", "name")))))),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p2", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("g2"))))),
                                                        project(ImmutableMap.of("g2", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("name_b1"), dataType("varchar")))))),
                                                                tableScan("region", ImmutableMap.of("name_b1", "name")))),
                                                project(ImmutableMap.of("p3", expression(new FunctionCall(QualifiedName.of("spatial_partitions"), ImmutableList.of(kdbTreeLiteral, new SymbolReference("g3"))))),
                                                        project(ImmutableMap.of("g3", expression(new FunctionCall(QualifiedName.of("st_geometryfromtext"), ImmutableList.of(new Cast(new SymbolReference("name_b2"), dataType("varchar")))))),
                                                                tableScan("nation", ImmutableMap.of("name_b2", "name"))))))))));
    }

    /**
     * Create SQL producing single row of given expressions using a table instead of VALUES.
     * Helps avoid VALUES-based constant folding.
     */
    private String singleRow(String... columns)
    {
        String outputs = String.join(", ", columns);
        return format("(SELECT %s FROM tpch.tiny.region WHERE regionkey = 1)", outputs);
    }

    /**
     * Match plan for the single row base subquery as created by `singleRow(String... columns)`
     */
    private PlanMatchPattern singleRow()
    {
        return filter(
                new ComparisonExpression(EQUAL, new SymbolReference("regionkey"), new GenericLiteral("BIGINT", "1")),
                tableScan("region", ImmutableMap.of("regionkey", "regionkey")));
    }

    private Session withSpatialPartitioning(String tableName)
    {
        return Session.builder(this.getPlanTester().getDefaultSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, tableName)
                .build();
    }

    private static String doubleLiteral(double value)
    {
        checkArgument(Double.isFinite(value));
        return format("%.16E", value);
    }

    private FunctionCall functionCall(String name, List<Type> types, List<Expression> arguments)
    {
        return new FunctionCall(getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction(name, fromTypes(types)).toQualifiedName(), arguments);
    }
}
