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

import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.SPATIAL_PARTITIONING_TABLE_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpatialJoins
        extends AbstractTestQueryFramework
{
    private static final String SRID_MISMATCH_ERROR = ".*SRID mismatch: (4326 vs 3857|3857 vs 4326).*";

    // A set of polygons such that:
    // - a and c intersect;
    // - c covers b;
    private static final String POLYGONS_SQL = "VALUES " +
            "('POLYGON ((-0.5 -0.6, 1.5 0, 1 1, 0 1, -0.5 -0.6))', 'a', 1), " +
            "('POLYGON ((2 2, 3 2, 2.5 3, 2 2))', 'b', 2), " +
            "('POLYGON ((0.8 0.7, 0.8 4, 5 4, 4.5 0.8, 0.8 0.7))', 'c', 3), " +
            "('POLYGON ((7 7, 11 7, 11 11, 7 7))', 'd', 4), " +
            "('POLYGON EMPTY', 'empty', 5), " +
            "(null, 'null', 6)";

    // A set of points such that:
    // - a contains x
    // - b and c contain y
    // - d contains z
    private static final String POINTS_SQL = "VALUES " +
            "(-0.1, -0.1, 'x', 1), " +
            "(2.1, 2.1, 'y', 2), " +
            "(7.1, 7.2, 'z', 3), " +
            "(null, 1.2, 'null', 4)";

    private static final String MULTI_POINTS_SQL = "VALUES " +
            "(-0.1, -0.1, 5.1, 5.1, 'x', 1), " +
            "(7.1, 7.1, 2.1, 2.1, 'y', 2), " +
            "(7.1, 7.2, 8, 9, 'z', 3), " +
            "(null, 1.2, 4, null, 'null', 4)";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource(TestSpatialJoins.class.getSimpleName())
                .setCatalog("hive")
                .setSchema("default")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new GeoPlugin());

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data")));
        queryRunner.createCatalog("hive", "hive");
        queryRunner.execute("CREATE SCHEMA hive.default");

        return queryRunner;
    }

    @Test
    public void testBroadcastSpatialJoinContains()
    {
        testSpatialJoinContains(getSession());
    }

    @Test
    public void testDistributedSpatialJoinContains()
    {
        assertUpdate(format("CREATE TABLE contains_partitioning AS " +
                "SELECT spatial_partitioning(ST_GeometryFromText(wkt)) as v " +
                "FROM (%s) as a (wkt, name, id)", POLYGONS_SQL), 1);

        Session session = Session.builder(getSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, "contains_partitioning")
                .build();
        testSpatialJoinContains(session);
    }

    private void testSpatialJoinContains(Session session)
    {
        // Test ST_Contains(build, probe)
        assertQuery(
                session,
                "SELECT b.name, a.name " +
                        "FROM (" + POINTS_SQL + ") AS a (latitude, longitude, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude))",
                "VALUES ('a', 'x'), ('b', 'y'), ('c', 'y'), ('d', 'z')");

        assertQuery(
                session,
                "SELECT b.name, a.name " +
                        "FROM (" + POINTS_SQL + ") AS a (latitude, longitude, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude))",
                "VALUES ('a', 'x'), ('b', 'y'), ('c', 'y'), ('d', 'z')");

        assertQuery(
                session,
                "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('c', 'b')");

        assertQuery(
                session,
                "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('c', 'b')");

        // Test ST_Contains(probe, build)
        assertQuery(
                session,
                "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS b (wkt, name, id), (" + POINTS_SQL + ") AS a (latitude, longitude, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude))",
                "VALUES ('a', 'x'), ('b', 'y'), ('c', 'y'), ('d', 'z')");

        assertQuery(
                session,
                "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('b', 'c')");
    }

    @Test
    public void testBroadcastSpatialJoinContainsWithExtraConditions()
    {
        assertQuery(
                "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) AND a.name != b.name",
                "VALUES ('c', 'b')");

        assertQuery(
                "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) AND a.name != b.name",
                "VALUES ('c', 'b')");
    }

    @Test
    public void testBroadcastSpatialJoinContainsWithStatefulExtraCondition()
    {
        // Generate multi-page probe input: 10K points in polygon 'a' and 10K points in polygon 'b'
        String pointsX = generatePointsSql(0, 0, 1, 1, 10_000, "x");
        String pointsY = generatePointsSql(2, 2, 2.5, 2.5, 10_000, "y");

        // Run spatial join with additional stateful filter
        assertQuery(
                "SELECT b.name, a.name " +
                        "FROM (" + pointsX + " UNION ALL " + pointsY + ") AS a (latitude, longitude, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude)) AND stateful_sleeping_sum(0.001, 100, a.id, b.id) <= 3",
                "VALUES ('a', 'x1'), ('a', 'x2'), ('b', 'y1')");
    }

    private static String generatePointsSql(double minX, double minY, double maxX, double maxY, int pointCount, String prefix)
    {
        return format(
                "SELECT %s + n * %f, %s + n * %f, '%s' || CAST (n AS VARCHAR), n " +
                        "FROM (SELECT sequence(1, %s) as numbers) " +
                        "CROSS JOIN UNNEST (numbers) AS t(n)",
                minX,
                (maxX - minX) / pointCount,
                minY,
                (maxY - minY) / pointCount,
                prefix,
                pointCount);
    }

    @Test
    public void testBroadcastSpatialJoinContainsWithEmptyBuildSide()
    {
        assertQueryReturnsEmptyResult("SELECT b.name, a.name " +
                "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                "WHERE b.name = 'invalid' AND ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))");
    }

    @Test
    public void testBroadcastSpatialJoinContainsWithEmptyProbeSide()
    {
        assertQueryReturnsEmptyResult("SELECT b.name, a.name " +
                "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                "WHERE a.name = 'invalid' AND ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))");
    }

    @Test
    public void testBroadcastSpatialJoinIntersects()
    {
        testSpatialJoinIntersects(getSession());
    }

    @Test
    public void tesDistributedSpatialJoinIntersects()
    {
        assertUpdate(format("CREATE TABLE intersects_partitioning AS " +
                "SELECT spatial_partitioning(ST_GeometryFromText(wkt)) as v " +
                "FROM (%s) as a (wkt, name, id)", POLYGONS_SQL), 1);

        Session session = Session.builder(getSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, "intersects_partitioning")
                .build();
        testSpatialJoinIntersects(session);
    }

    private void testSpatialJoinIntersects(Session session)
    {
        // Test ST_Intersects(build, probe)
        assertQuery(
                session,
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        assertQuery(
                session,
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        // Test ST_Intersects(probe, build)
        assertQuery(
                session,
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");
    }

    @Test
    public void testBroadcastSpatialJoinIntersectsWithExtraConditions()
    {
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) " +
                        "   AND a.name != b.name",
                "VALUES ('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) " +
                        "   AND a.name != b.name",
                "VALUES ('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) " +
                        "   AND a.name < b.name",
                "VALUES ('a', 'c'), ('b', 'c')");
    }

    @Test
    public void testBroadcastDistanceQuery()
    {
        testDistanceQuery(getSession());
    }

    @Test
    public void testDistributedDistanceQuery()
    {
        assertUpdate("CREATE TABLE distance_partitioning AS SELECT spatial_partitioning(ST_Point(x, y)) as v " +
                "FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name)", 1);

        Session session = Session.builder(getSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, "distance_partitioning")
                .build();
        testDistanceQuery(session);
    }

    private void testDistanceQuery(Session session)
    {
        // ST_Distance(probe, build)
        assertQuery(
                session,
                "SELECT a.name, b.name " +
                        "FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name), " +
                        "(VALUES (0, 1, '0_1'), (1, 1, '1_1'), (3, 1, '3_1'), (10, 1, '10_1')) as b (x, y, name) " +
                        "WHERE ST_Distance(ST_Point(a.x, a.y), ST_Point(b.x, b.y)) <= 1.5",
                "VALUES ('0_0', '0_1'), ('0_0', '1_1'), ('1_0', '0_1'), ('1_0', '1_1'), ('3_0', '3_1'), ('10_0', '10_1')");

        // ST_Distance(build, probe)
        assertQuery(
                session,
                "SELECT a.name, b.name " +
                        "FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name), " +
                        "(VALUES (0, 1, '0_1'), (1, 1, '1_1'), (3, 1, '3_1'), (10, 1, '10_1')) as b (x, y, name) " +
                        "WHERE ST_Distance(ST_Point(b.x, b.y), ST_Point(a.x, a.y)) <= 1.5",
                "VALUES ('0_0', '0_1'), ('0_0', '1_1'), ('1_0', '0_1'), ('1_0', '1_1'), ('3_0', '3_1'), ('10_0', '10_1')");

        // radius expression
        assertQuery(
                session,
                "SELECT a.name, b.name FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name), " +
                        "(VALUES (0, 1, '0_1'), (1, 1, '1_1'), (3, 1, '3_1'), (10, 1, '10_1')) as b (x, y, name) " +
                        "WHERE ST_Distance(ST_Point(a.x, a.y), ST_Point(b.x, b.y)) <= sqrt(b.x * b.x + b.y * b.y)",
                "VALUES ('0_0', '0_1'), ('0_0', '1_1'), ('0_0', '3_1'), ('0_0', '10_1'), ('1_0', '1_1'), ('1_0', '3_1'), ('1_0', '10_1'), ('3_0', '3_1'), ('3_0', '10_1'), ('10_0', '10_1')");
    }

    @Test
    public void testBroadcastSpatialJoinSridMismatch()
    {
        testSpatialJoinSridMismatch(getSession());
    }

    @Test
    public void testDistributedSpatialJoinSridMismatch()
    {
        assertUpdate(
                "CREATE TABLE srid_mismatch_partitioning AS " +
                        "SELECT spatial_partitioning(g) AS v " +
                        "FROM (VALUES " +
                        "(ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'), 4326)), " +
                        "(ST_SetSRID(ST_Point(1, 1), 4326))) AS a (g)",
                1);

        Session session = Session.builder(getSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, "srid_mismatch_partitioning")
                .build();
        testSpatialJoinSridMismatch(session);
    }

    private void testSpatialJoinSridMismatch(Session session)
    {
        assertQueryFails(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_SetSRID(ST_Point(1, 1), 3857))) AS b (g) " +
                        "ON ST_Contains(a.g, b.g)",
                SRID_MISMATCH_ERROR);

        assertQueryFails(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_SetSRID(ST_Point(1, 1), 3857))) AS b (g) " +
                        "ON ST_Within(b.g, a.g)",
                SRID_MISMATCH_ERROR);

        assertQueryFails(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_SetSRID(ST_Point(1, 1), 3857))) AS b (g) " +
                        "ON ST_Intersects(a.g, b.g)",
                SRID_MISMATCH_ERROR);

        assertQueryFails(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_Point(0, 0), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_SetSRID(ST_Point(0, 1), 3857))) AS b (g) " +
                        "ON ST_Distance(a.g, b.g) <= 2",
                SRID_MISMATCH_ERROR);

        assertQueryFails(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_SetSRID(ST_Point(100, 100), 3857))) AS b (g) " +
                        "ON ST_Contains(a.g, b.g)",
                SRID_MISMATCH_ERROR);

        assertQueryFails(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_Point(0, 0), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_SetSRID(ST_Point(100, 100), 3857))) AS b (g) " +
                        "ON ST_Distance(a.g, b.g) <= 1",
                SRID_MISMATCH_ERROR);

        assertQueryReturnsEmptyResult(
                session,
                "SELECT a.g, b.g " +
                        "FROM (VALUES (ST_SetSRID(ST_Point(0, 0), 4326))) AS a (g) " +
                        "JOIN (VALUES (ST_Point(100, 100))) AS b (g) " +
                        "ON ST_Distance(a.g, b.g) <= 1");
    }

    @Test
    public void testBroadcastSpatialJoinWithZAndSrid()
    {
        testSpatialJoinWithZAndSrid(getSession());
    }

    @Test
    public void testDistributedSpatialJoinWithZAndSrid()
    {
        assertUpdate(
                "CREATE TABLE z_srid_partitioning AS " +
                        "SELECT spatial_partitioning(g) AS v " +
                        "FROM (VALUES " +
                        "(ST_SetSRID(ST_GeometryFromText('POLYGON Z ((0 0 1, 0 5 2, 5 5 3, 5 0 4, 0 0 1))'), 4326)), " +
                        "(ST_SetSRID(ST_GeometryFromText('POLYGON Z ((4 4 10, 4 10 20, 10 10 30, 10 4 40, 4 4 10))'), 4326))) AS a (g)",
                1);

        Session session = Session.builder(getSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, "z_srid_partitioning")
                .build();
        testSpatialJoinWithZAndSrid(session);
    }

    private void testSpatialJoinWithZAndSrid(Session session)
    {
        assertQuery(
                session,
                "SELECT point_name, polygon_name " +
                        "FROM (VALUES " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (1 1 99)'), 4326), 'x'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (4.5 4.5 -7)'), 4326), 'y'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (6 6 123)'), 4326), 'z'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (20 20 0)'), 4326), 'w')) AS points(g, point_name) " +
                        "JOIN (VALUES " +
                        "(ST_SetSRID(ST_GeometryFromText('POLYGON Z ((0 0 1, 0 5 2, 5 5 3, 5 0 4, 0 0 1))'), 4326), 'A'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POLYGON Z ((4 4 10, 4 10 20, 10 10 30, 10 4 40, 4 4 10))'), 4326), 'B')) AS polygons(g, polygon_name) " +
                        "ON ST_Contains(polygons.g, points.g)",
                "VALUES ('x', 'A'), ('y', 'A'), ('y', 'B'), ('z', 'B')");

        assertQuery(
                session,
                "SELECT probe_name, build_name " +
                        "FROM (VALUES " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (0 1 -100)'), 4326), '0_1'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (1 1 -200)'), 4326), '1_1'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (3 1 -300)'), 4326), '3_1'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (10 1 -400)'), 4326), '10_1')) AS probe(g, probe_name) " +
                        "JOIN (VALUES " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (0 0 100)'), 4326), '0_0'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (1 0 200)'), 4326), '1_0'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (3 0 300)'), 4326), '3_0'), " +
                        "(ST_SetSRID(ST_GeometryFromText('POINT Z (10 0 400)'), 4326), '10_0')) AS build(g, build_name) " +
                        "ON ST_Distance(probe.g, build.g) <= 1.5",
                "VALUES ('0_1', '0_0'), ('0_1', '1_0'), ('1_1', '0_0'), ('1_1', '1_0'), ('3_1', '3_0'), ('10_1', '10_0')");
    }

    @Test
    public void testBroadcastSpatialLeftJoin()
    {
        // Test ST_Intersects(build, probe)
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) LEFT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c'), ('empty', null), ('null', null)");

        // Empty build side
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) LEFT JOIN (VALUES (null, 'null', 1)) AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', null), ('b', null), ('c', null), ('d', null), ('empty', null), ('null', null)");

        // Extra condition
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) LEFT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON a.name > b.name AND ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', null), ('b', null), ('c', 'a'), ('c', 'b'), ('d', null), ('empty', null), ('null', null)");
    }

    @Test
    public void testBroadcastSpatialRightJoin()
    {
        // Test ST_Intersects(build, probe)
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) RIGHT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c'), (null, 'empty'), (null, 'null')");

        // Empty build side
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) RIGHT JOIN (VALUES (null, 'null', 1)) AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES (null, 'null')");

        // Extra condition
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) RIGHT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON a.name > b.name AND ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES (null, 'c'), (null, 'd'), ('c', 'a'), ('c', 'b'), (null, 'empty'), (null, 'null')");
    }

    @Test
    public void testSpatialJoinOverRightJoin()
    {
        assertQuery(
                "SELECT a.name, b.name, c.name " +
                        "FROM (" + POINTS_SQL + ") AS a (latitude, longitude, name, id) " +
                        "RIGHT JOIN (" + POINTS_SQL + ") AS b (latitude, longitude, name, id) ON a.latitude = b.latitude AND a.longitude = b.longitude AND a.latitude > 0 " +
                        "JOIN (" + POINTS_SQL + ") AS c (latitude, longitude, name, id) ON ST_Distance(ST_Point(a.latitude, b.longitude), ST_Point(c.latitude, c.longitude)) < 1 ",
                "VALUES ('y', 'y', 'y'), ('z', 'z', 'z')");
    }

    @Test
    public void testSpatialJoinOverLeftJoinWithOrPredicate()
    {
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "LEFT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b') , ('y', 'c'), ('z', NULL), ('null', NULL)");
    }

    @Test
    public void testSpatialJoinOverRightJoinWithOrPredicate()
    {
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "RIGHT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b') , ('y', 'c'), (NULL, 'd'), (NULL, 'empty'), (NULL, 'null')");
    }

    @Test
    public void testSpatialJoinOverInnerJoinWithOrPredicate()
    {
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "INNER JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b') , ('y', 'c')");
    }

    @Test
    public void testSpatialJoinOverFullJoinWithOrPredicate()
    {
        assertQuery(
                "SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "FULL JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b'), ('y', 'c'), (NULL, 'd'), (NULL, 'empty'), ('z', NULL), (NULL, 'null'), ('null', NULL)");
    }

    @Test
    public void testLeftJoin()
    {
        assertThat(new QueryAssertions(getQueryRunner()).query(
                """
                WITH
                    points(lat, lon) AS ( VALUES (0.5, 0.5), (2, 2) ),
                    polygons(id, x) AS ( VALUES (1, ST_GeometryFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')) )
                SELECT id, lat, lon
                FROM points LEFT JOIN polygons ON st_contains(x, ST_Point(lat, lon))
                """))
                .matches(
                        """
                        VALUES
                            (1, 0.5, 0.5),
                            (NULL, 2, 2)
                        """);
    }
}
