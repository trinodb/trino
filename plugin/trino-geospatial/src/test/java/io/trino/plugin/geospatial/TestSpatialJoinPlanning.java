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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.memory.MemoryConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.SystemSessionProperties.SPATIAL_PARTITIONING_TABLE_NAME;
import static io.trino.geospatial.KdbTree.Node.newLeaf;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.geospatial.KdbTreeType.KDB_TREE;
import static io.trino.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
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
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSpatialJoinPlanning
        extends BasePlanTest
{
    private static final String KDB_TREE_JSON = KdbTreeUtils.toJson(new KdbTree(newLeaf(new Rectangle(0, 0, 10, 10), 0)));
    private static final Expression KDB_TREE_LITERAL = new Constant(KDB_TREE, KdbTreeUtils.fromJson(KDB_TREE_JSON));

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(new GeoPlugin());
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());
    private static final ResolvedFunction SPATIAL_PARTITIONS = FUNCTIONS.resolveFunction("spatial_partitions", fromTypes(KDB_TREE, GEOMETRY));
    private static final ResolvedFunction ST_CONTAINS = FUNCTIONS.resolveFunction("st_contains", fromTypes(GEOMETRY, GEOMETRY));
    private static final ResolvedFunction ST_INTERSECTS = FUNCTIONS.resolveFunction("st_intersects", fromTypes(GEOMETRY, GEOMETRY));
    private static final ResolvedFunction ST_WITHIN = FUNCTIONS.resolveFunction("st_within", fromTypes(GEOMETRY, GEOMETRY));
    private static final ResolvedFunction ST_POINT = FUNCTIONS.resolveFunction("st_point", fromTypes(DOUBLE, DOUBLE));
    private static final ResolvedFunction ST_GEOMETRY_FROM_TEXT = FUNCTIONS.resolveFunction("st_geometryfromtext", fromTypes(VARCHAR));
    private static final ResolvedFunction LENGTH = FUNCTIONS.resolveFunction("length", fromTypes(VARCHAR));
    private static final ResolvedFunction CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR));

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

    @Test
    public void testSpatialJoinContains()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(
                                new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                project(ImmutableMap.of(
                                                "st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat")))),
                                                "length", expression(new Call(LENGTH, ImmutableList.of(new Reference(VARCHAR, "name"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of(
                                                        "st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR)))),
                                                        "length_2", expression(new Call(LENGTH, ImmutableList.of(new Reference(VARCHAR, "name_2"))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(
                                new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                Optional.of(KDB_TREE_JSON),
                                Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_a", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "st_point"))))),
                                                        project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_b", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "st_geometryfromtext"))))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
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
                                new Call(ST_WITHIN, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin(new Call(ST_WITHIN, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                project(ImmutableMap.of(
                                                "st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat")))),
                                                "length", expression(new Call(LENGTH, ImmutableList.of(new Reference(VARCHAR, "name"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of(
                                                        "st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR)))),
                                                        "length_2", expression(new Call(LENGTH, ImmutableList.of(new Reference(VARCHAR, "name_2"))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(
                                new Call(ST_WITHIN, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                Optional.of(KDB_TREE_JSON),
                                Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_a", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "st_point"))))),
                                                        project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_b", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "st_geometryfromtext"))))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
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
                                new Call(ST_INTERSECTS, ImmutableList.of(new Reference(GEOMETRY, "geometry_a"), new Reference(GEOMETRY, "geometry_b"))),
                                project(ImmutableMap.of("geometry_a", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_a"), VARCHAR))))),
                                        tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("geometry_b", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_b"), VARCHAR))))),
                                                tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                withSpatialPartitioning("default.kdb_tree"),
                anyTree(
                        spatialJoin(
                                new Call(ST_INTERSECTS, ImmutableList.of(new Reference(GEOMETRY, "geometry_a"), new Reference(GEOMETRY, "geometry_b"))), Optional.of(KDB_TREE_JSON), Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions_a", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "geometry_a"))))),
                                                        project(ImmutableMap.of("geometry_a", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_a"), VARCHAR))))),
                                                                tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name")))))),
                                anyTree(
                                        project(ImmutableMap.of("partitions_b", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "geometry_b"))))),
                                                project(ImmutableMap.of("geometry_b", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_b"), VARCHAR))))),
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
                                new Not(new Call(ST_CONTAINS, ImmutableList.of(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))), new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat")))))),
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
                                new Not(
                                        functionCall("ST_Intersects", ImmutableList.of(GEOMETRY, GEOMETRY), ImmutableList.of(
                                                functionCall("ST_GeometryFromText", ImmutableList.of(VARCHAR), ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_a"), VARCHAR))),
                                                functionCall("ST_GeometryFromText", ImmutableList.of(VARCHAR), ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_b"), VARCHAR)))))),
                                join(INNER, builder -> builder
                                        .left(
                                                project(
                                                        ImmutableMap.of(
                                                                "wkt_a", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN_OR_EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.0)), new Constant(createVarcharType(45), Slices.utf8Slice("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")))), new Constant(createVarcharType(45), null))),
                                                                "name_a", expression(new Constant(createVarcharType(1), Slices.utf8Slice("a")))),
                                                        singleRow()))
                                        .right(
                                                any(project(
                                                        ImmutableMap.of(
                                                                "wkt_b", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN_OR_EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.0)), new Constant(createVarcharType(45), Slices.utf8Slice("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")))), new Constant(createVarcharType(45), null))),
                                                                "name_b", expression(new Constant(createVarcharType(1), Slices.utf8Slice("a")))),
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
                                .filter(new Call(ST_CONTAINS, ImmutableList.of(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))), new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))))
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
                                .filter(new Call(ST_INTERSECTS, ImmutableList.of(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_a"), VARCHAR))), new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt_b"), VARCHAR))))))
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
                                new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND a.name <> b.name",
                anyTree(
                        spatialLeftJoin(
                                new Logical(AND, ImmutableList.of(new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))), new Comparison(NOT_EQUAL, new Reference(VARCHAR, "name_a"), new Reference(VARCHAR, "name_b")))),
                                project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // non-deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND rand() < 0.5",
                anyTree(
                        spatialLeftJoin(
                                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.5)), new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))))),
                                project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // filter over join
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "   ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) " +
                        "WHERE concat(a.name, b.name) is null",
                anyTree(
                        filter(
                                new IsNull(new Call(CONCAT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_a"), VARCHAR), new Cast(new Reference(VARCHAR, "name_b"), VARCHAR)))),
                                spatialLeftJoin(
                                        new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "st_geometryfromtext"), new Reference(GEOMETRY, "st_point"))),
                                        project(ImmutableMap.of("st_point", expression(new Call(ST_POINT, ImmutableList.of(new Reference(DOUBLE, "lng"), new Reference(DOUBLE, "lat"))))),
                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                        anyTree(
                                                project(ImmutableMap.of("st_geometryfromtext", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "wkt"), VARCHAR))))),
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
                        spatialJoin(new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "g1"), new Reference(GEOMETRY, "g3"))), Optional.of(KDB_TREE_JSON), Optional.empty(),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p1", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "g1"))))),
                                                        project(ImmutableMap.of("g1", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_a1"), VARCHAR))))),
                                                                tableScan("region", ImmutableMap.of("name_a1", "name")))),
                                                project(ImmutableMap.of("p2", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "g2"))))),
                                                        project(ImmutableMap.of("g2", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_a2"), VARCHAR))))),
                                                                tableScan("nation", ImmutableMap.of("name_a2", "name"))))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p3", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "g3"))))),
                                                        project(ImmutableMap.of("g3", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_b"), VARCHAR))))),
                                                                tableScan("customer", ImmutableMap.of("name_b", "name")))))))));

        // union on the right side
        assertDistributedPlan("SELECT a.name, b.name " +
                        "FROM tpch.tiny.customer a, (SELECT name FROM tpch.tiny.region UNION ALL SELECT name FROM tpch.tiny.nation) b " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.name), ST_GeometryFromText(b.name))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin(
                                new Call(ST_CONTAINS, ImmutableList.of(new Reference(GEOMETRY, "g1"), new Reference(GEOMETRY, "g2"))),
                                Optional.of(KDB_TREE_JSON),
                                Optional.empty(),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p1", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "g1"))))),
                                                        project(ImmutableMap.of("g1", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_a"), VARCHAR))))),
                                                                tableScan("customer", ImmutableMap.of("name_a", "name")))))),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p2", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "g2"))))),
                                                        project(ImmutableMap.of("g2", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_b1"), VARCHAR))))),
                                                                tableScan("region", ImmutableMap.of("name_b1", "name")))),
                                                project(ImmutableMap.of("p3", expression(new Call(SPATIAL_PARTITIONS, ImmutableList.of(KDB_TREE_LITERAL, new Reference(GEOMETRY, "g3"))))),
                                                        project(ImmutableMap.of("g3", expression(new Call(ST_GEOMETRY_FROM_TEXT, ImmutableList.of(new Cast(new Reference(VARCHAR, "name_b2"), VARCHAR))))),
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
                new Comparison(EQUAL, new Reference(BIGINT, "regionkey"), new Constant(BIGINT, 1L)),
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

    private Call functionCall(String name, List<Type> types, List<Expression> arguments)
    {
        return new Call(getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction(name, fromTypes(types)), arguments);
    }
}
