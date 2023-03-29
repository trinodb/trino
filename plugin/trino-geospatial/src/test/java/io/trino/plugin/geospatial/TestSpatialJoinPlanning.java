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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.trino.Session;
import io.trino.block.BlockSerdeUtil;
import io.trino.execution.warnings.WarningCollector;
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.plugin.memory.MemoryConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Base64;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.SystemSessionProperties.SPATIAL_PARTITIONING_TABLE_NAME;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.geospatial.KdbTree.Node.newLeaf;
import static io.trino.metadata.LiteralFunction.LITERAL_FUNCTION_NAME;
import static io.trino.plugin.geospatial.GeoFunctions.stPoint;
import static io.trino.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
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
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.cos;
import static java.lang.Math.toRadians;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSpatialJoinPlanning
        extends BasePlanTest
{
    private static final String KDB_TREE_JSON = KdbTreeUtils.toJson(new KdbTree(newLeaf(new Rectangle(0, 0, 10, 10), 0)));

    private String kdbTreeLiteral;
    private String point21x21Literal;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build());
        queryRunner.installPlugin(new GeoPlugin());
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        queryRunner.createCatalog("memory", new MemoryConnectorFactory(), ImmutableMap.of());
        queryRunner.execute(format("CREATE TABLE kdb_tree AS SELECT '%s' AS v", KDB_TREE_JSON));
        queryRunner.execute("CREATE TABLE points (lng, lat, name) AS (VALUES (2.1e0, 2.1e0, 'x'))");
        queryRunner.execute("CREATE TABLE polygons (wkt, name) AS (VALUES ('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))', 'a'))");
        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        Block block = nativeValueToBlock(KdbTreeType.KDB_TREE, KdbTreeUtils.fromJson(KDB_TREE_JSON));
        DynamicSliceOutput output = new DynamicSliceOutput(0);
        BlockSerdeUtil.writeBlock(new TestingBlockEncodingSerde(), output, block);
        kdbTreeLiteral = format("\"%s\"(from_base64('%s'))", LITERAL_FUNCTION_NAME, Base64.getEncoder().encodeToString(output.slice().getBytes()));
        point21x21Literal = format("\"%s\"(from_base64('%s'))", LITERAL_FUNCTION_NAME, Base64.getEncoder().encodeToString(stPoint(2.1, 2.1).getBytes()));
    }

    @Test
    public void testSpatialJoinContains()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)"), "length", expression("length(name)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))"), "length_2", expression("length(name_2)")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_contains(st_geometryfromtext, st_point)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(%s, st_point)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(%s, st_geometryfromtext)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
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
                        spatialJoin("st_within(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_within(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)"), "length", expression("length(name)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))"), "length_2", expression("length(name_2)")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_within(st_geometryfromtext, st_point)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(%s, st_point)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(%s, st_geometryfromtext)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
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
        getQueryRunner().execute("CREATE TABLE empty_table AS SELECT 'a' AS v WHERE false");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("empty_table"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected exactly one row for table memory.default.empty_table, but got none");

        // invalid JSON
        getQueryRunner().execute("CREATE TABLE invalid_kdb_tree AS SELECT 'invalid-json' AS v");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("invalid_kdb_tree"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Invalid JSON string for KDB tree: .*");

        // more than one row
        getQueryRunner().execute(format("CREATE TABLE too_many_rows AS SELECT * FROM (VALUES '%s', '%s') AS t(v)", KDB_TREE_JSON, KDB_TREE_JSON));

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("too_many_rows"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected exactly one row for table memory.default.too_many_rows, but found 2 rows");

        // more than one column
        getQueryRunner().execute("CREATE TABLE too_many_columns AS SELECT '%s' as c1, 100 as c2");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("too_many_columns"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected single column for table memory.default.too_many_columns, but found 2 columns");
    }

    private void assertInvalidSpatialPartitioning(Session session, String sql, String expectedMessageRegExp)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.inTransaction(session, transactionSession -> {
                queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
                return null;
            });
            fail(format("Expected query to fail: %s", sql));
        }
        catch (TrinoException ex) {
            assertEquals(ex.getErrorCode(), INVALID_SPATIAL_PARTITIONING.toErrorCode());
            if (!nullToEmpty(ex.getMessage()).matches(expectedMessageRegExp)) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", ex.getMessage(), expectedMessageRegExp, sql), ex);
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
                        spatialJoin("st_intersects(geometry_a, geometry_b)",
                                project(ImmutableMap.of("geometry_a", expression("ST_GeometryFromText(cast(wkt_a as varchar))")),
                                        tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("geometry_b", expression("ST_GeometryFromText(cast(wkt_b as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                withSpatialPartitioning("default.kdb_tree"),
                anyTree(
                        spatialJoin("st_intersects(geometry_a, geometry_b)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(%s, geometry_a)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("geometry_a", expression("ST_GeometryFromText(cast(wkt_a as varchar))")),
                                                                tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name")))))),
                                anyTree(
                                        project(ImmutableMap.of("partitions", expression(format("spatial_partitions(%s, geometry_b)", kdbTreeLiteral))),
                                                project(ImmutableMap.of("geometry_b", expression("ST_GeometryFromText(cast(wkt_b as varchar))")),
                                                        tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name"))))))));
    }

    @Test
    public void testDistanceQuery()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM " + singleRow("2.1", "2.1", "'x'") + " AS a (lng, lat, name), " + singleRow("2.1", "2.1", "'x'") + " AS b (lng, lat, name) " +
                        "WHERE ST_Distance(ST_Point(a.lng, a.lat), ST_Point(b.lng, b.lat)) <= 3.1",
                anyTree(
                        spatialJoin("st_distance(st_point_a, st_point_b) <= radius",
                                project(
                                        ImmutableMap.of("st_point_a", expression(point21x21Literal), "a_name", expression("'x'")),
                                        singleRow()),
                                any(
                                        project(
                                                ImmutableMap.of("st_point_b", expression(point21x21Literal), "radius", expression("3.1e0"), "b_name", expression("'x'")),
                                                singleRow())))));

        assertPlan("SELECT b.name, a.name " +
                        "FROM " + singleRow("2.1", "2.1", "'x'") + " AS a (lng, lat, name), " + singleRow("2.1", "2.1", "'x'") + " AS b (lng, lat, name) " +
                        "WHERE ST_Distance(ST_Point(a.lng, a.lat), ST_Point(b.lng, b.lat)) <= 300 / (cos(radians(b.lat)) * 111321)",
                anyTree(
                        spatialJoin("st_distance(st_point_a, st_point_b) <= radius",
                                project(
                                        ImmutableMap.of("st_point_a", expression(point21x21Literal), "a_name", expression("'x'")),
                                        singleRow()),
                                any(
                                        project(
                                                ImmutableMap.of("st_point_b", expression(point21x21Literal), "radius", expression(doubleLiteral(3e2 / (cos(toRadians(2.1)) * 111.321e3))), "b_name", expression("'x'")),
                                                singleRow())))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM " + singleRow("2.1", "2.1", "'x'") + " AS a (lng, lat, name), " + singleRow("2.1", "2.1", "'x'") + " AS b (lng, lat, name) " +
                        "WHERE ST_Distance(ST_Point(a.lng, a.lat), ST_Point(b.lng, b.lat)) <= 3.1",
                withSpatialPartitioning("memory.default.kdb_tree"),
                anyTree(
                        spatialJoin("st_distance(st_point_a, st_point_b) <= radius", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(
                                                        ImmutableMap.of(
                                                                "st_point_a", expression(point21x21Literal),
                                                                "partitions", expression(format("spatial_partitions(%s, %s)", kdbTreeLiteral, point21x21Literal))),
                                                        singleRow()))),
                                anyTree(
                                        unnest(
                                                project(
                                                        ImmutableMap.of(
                                                                "st_point_b", expression(point21x21Literal),
                                                                "partitions", expression(format("spatial_partitions(%s, %s, 3.1e0)", kdbTreeLiteral, point21x21Literal)),
                                                                "radius", expression("3.1e0")),
                                                        singleRow()))))));
    }

    @Test
    public void testNotContains()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE NOT ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        filter("NOT ST_Contains(ST_GeometryFromText(cast(wkt as varchar)), ST_Point(lng, lat))",
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
                                "NOT ST_Intersects(ST_GeometryFromText(cast(wkt_a as varchar)), ST_GeometryFromText(cast(wkt_b as varchar)))",
                                join(INNER, builder -> builder
                                        .left(
                                                project(
                                                        ImmutableMap.of("wkt_a", expression("(CASE WHEN (rand() >= 0E0) THEN 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))' END)"), "name_a", expression("'a'")),
                                                        singleRow()))
                                        .right(
                                                any(project(
                                                        ImmutableMap.of("wkt_b", expression("(CASE WHEN (rand() >= 0E0) THEN 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))' END)"), "name_b", expression("'a'")),
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
                                .filter("ST_Contains(ST_GeometryFromText(cast(wkt as varchar)), ST_Point(lng, lat))")
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
                                .filter("ST_Intersects(ST_GeometryFromText(cast(wkt_a as varchar)), ST_GeometryFromText(cast(wkt_b as varchar)))")
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
                        spatialLeftJoin("st_contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND a.name <> b.name",
                anyTree(
                        spatialLeftJoin("st_contains(st_geometryfromtext, st_point) AND name_a <> name_b",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // non-deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND rand() < 0.5",
                anyTree(
                        spatialLeftJoin("st_contains(st_geometryfromtext, st_point) AND rand() < 5e-1",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // filter over join
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "   ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) " +
                        "WHERE concat(a.name, b.name) is null",
                anyTree(
                        filter("concat(cast(name_a as varchar), cast(name_b as varchar)) is null",
                                spatialLeftJoin("st_contains(st_geometryfromtext, st_point)",
                                        project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                        anyTree(
                                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
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
                        spatialJoin("st_contains(g1, g3)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p1", expression(format("spatial_partitions(%s, g1)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("g1", expression("ST_GeometryFromText(cast(name_a1 as varchar))")),
                                                                tableScan("region", ImmutableMap.of("name_a1", "name")))),
                                                project(ImmutableMap.of("p2", expression(format("spatial_partitions(%s, g2)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("g2", expression("ST_GeometryFromText(cast(name_a2 as varchar))")),
                                                                tableScan("nation", ImmutableMap.of("name_a2", "name"))))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p3", expression(format("spatial_partitions(%s, g3)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("g3", expression("ST_GeometryFromText(cast(name_b as varchar))")),
                                                                tableScan("customer", ImmutableMap.of("name_b", "name")))))))));

        // union on the right side
        assertDistributedPlan("SELECT a.name, b.name " +
                        "FROM tpch.tiny.customer a, (SELECT name FROM tpch.tiny.region UNION ALL SELECT name FROM tpch.tiny.nation) b " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.name), ST_GeometryFromText(b.name))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_contains(g1, g2)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p1", expression(format("spatial_partitions(%s, g1)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("g1", expression("ST_GeometryFromText(cast(name_a as varchar))")),
                                                                tableScan("customer", ImmutableMap.of("name_a", "name")))))),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p2", expression(format("spatial_partitions(%s, g2)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("g2", expression("ST_GeometryFromText(cast(name_b1 as varchar))")),
                                                                tableScan("region", ImmutableMap.of("name_b1", "name")))),
                                                project(ImmutableMap.of("p3", expression(format("spatial_partitions(%s, g3)", kdbTreeLiteral))),
                                                        project(ImmutableMap.of("g3", expression("ST_GeometryFromText(cast(name_b2 as varchar))")),
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
                "regionkey = BIGINT '1'",
                tableScan("region", ImmutableMap.of("regionkey", "regionkey")));
    }

    private Session withSpatialPartitioning(String tableName)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, tableName)
                .build();
    }

    private static String doubleLiteral(double value)
    {
        checkArgument(Double.isFinite(value));
        return format("%.16E", value);
    }
}
