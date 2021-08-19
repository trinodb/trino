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
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.SystemSessionProperties.SPATIAL_PARTITIONING_TABLE_NAME;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestSpatialJoins
        extends AbstractTestQueryFramework
{
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
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource(TestSpatialJoins.class.getSimpleName())
                .setCatalog("hive")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new GeoPlugin());

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();

        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);

        metastore.createDatabase(
                new HiveIdentity(SESSION),
                Database.builder()
                        .setDatabaseName("default")
                        .setOwnerName("public")
                        .setOwnerType(PrincipalType.ROLE)
                        .build());
        queryRunner.installPlugin(new TestingHivePlugin(metastore));

        queryRunner.createCatalog("hive", "hive");
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
        assertQuery(session, "SELECT b.name, a.name " +
                        "FROM (" + POINTS_SQL + ") AS a (latitude, longitude, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude))",
                "VALUES ('a', 'x'), ('b', 'y'), ('c', 'y'), ('d', 'z')");

        assertQuery(session, "SELECT b.name, a.name " +
                        "FROM (" + POINTS_SQL + ") AS a (latitude, longitude, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude))",
                "VALUES ('a', 'x'), ('b', 'y'), ('c', 'y'), ('d', 'z')");

        assertQuery(session, "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('c', 'b')");

        assertQuery(session, "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('c', 'b')");

        // Test ST_Contains(probe, build)
        assertQuery(session, "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS b (wkt, name, id), (" + POINTS_SQL + ") AS a (latitude, longitude, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(longitude, latitude))",
                "VALUES ('a', 'x'), ('b', 'y'), ('c', 'y'), ('d', 'z')");

        assertQuery(session, "SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('b', 'c')");
    }

    @Test
    public void testBroadcastSpatialJoinContainsWithExtraConditions()
    {
        assertQuery("SELECT b.name, a.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Contains(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) AND a.name != b.name",
                "VALUES ('c', 'b')");

        assertQuery("SELECT b.name, a.name " +
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
        assertQuery("SELECT b.name, a.name " +
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
        assertQuery(session, "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        assertQuery(session, "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        // Test ST_Intersects(probe, build)
        assertQuery(session, "SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");
    }

    @Test
    public void testBroadcastSpatialJoinIntersectsWithExtraConditions()
    {
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id), (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "WHERE ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) " +
                        "   AND a.name != b.name",
                "VALUES ('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt)) " +
                        "   AND a.name != b.name",
                "VALUES ('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c')");

        assertQuery("SELECT a.name, b.name " +
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
        assertQuery(session, "SELECT a.name, b.name " +
                        "FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name), " +
                        "(VALUES (0, 1, '0_1'), (1, 1, '1_1'), (3, 1, '3_1'), (10, 1, '10_1')) as b (x, y, name) " +
                        "WHERE ST_Distance(ST_Point(a.x, a.y), ST_Point(b.x, b.y)) <= 1.5",
                "VALUES ('0_0', '0_1'), ('0_0', '1_1'), ('1_0', '0_1'), ('1_0', '1_1'), ('3_0', '3_1'), ('10_0', '10_1')");

        // ST_Distance(build, probe)
        assertQuery(session, "SELECT a.name, b.name " +
                        "FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name), " +
                        "(VALUES (0, 1, '0_1'), (1, 1, '1_1'), (3, 1, '3_1'), (10, 1, '10_1')) as b (x, y, name) " +
                        "WHERE ST_Distance(ST_Point(b.x, b.y), ST_Point(a.x, a.y)) <= 1.5",
                "VALUES ('0_0', '0_1'), ('0_0', '1_1'), ('1_0', '0_1'), ('1_0', '1_1'), ('3_0', '3_1'), ('10_0', '10_1')");

        // radius expression
        assertQuery(session, "SELECT a.name, b.name FROM (VALUES (0, 0, '0_0'), (1, 0, '1_0'), (3, 0, '3_0'), (10, 0, '10_0')) as a (x, y, name), " +
                        "(VALUES (0, 1, '0_1'), (1, 1, '1_1'), (3, 1, '3_1'), (10, 1, '10_1')) as b (x, y, name) " +
                        "WHERE ST_Distance(ST_Point(a.x, a.y), ST_Point(b.x, b.y)) <= sqrt(b.x * b.x + b.y * b.y)",
                "VALUES ('0_0', '0_1'), ('0_0', '1_1'), ('0_0', '3_1'), ('0_0', '10_1'), ('1_0', '1_1'), ('1_0', '3_1'), ('1_0', '10_1'), ('3_0', '3_1'), ('3_0', '10_1'), ('10_0', '10_1')");
    }

    @Test
    public void testBroadcastSpatialLeftJoin()
    {
        // Test ST_Intersects(build, probe)
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) LEFT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "SELECT * FROM VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), " +
                        "('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c'), ('empty', null), ('null', null)");

        // Empty build side
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) LEFT JOIN (VALUES (null, 'null', 1)) AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', null), ('b', null), ('c', null), ('d', null), ('empty', null), ('null', null)");

        // Extra condition
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) LEFT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON a.name > b.name AND ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', null), ('b', null), ('c', 'a'), ('c', 'b'), ('d', null), ('empty', null), ('null', null)");
    }

    @Test
    public void testBroadcastSpatialRightJoin()
    {
        // Test ST_Intersects(build, probe)
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) RIGHT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES ('a', 'a'), ('b', 'b'), ('c', 'c'), ('d', 'd'), ('a', 'c'), ('c', 'a'), ('c', 'b'), ('b', 'c'), (null, 'empty'), (null, 'null')");

        // Empty build side
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) RIGHT JOIN (VALUES (null, 'null', 1)) AS b (wkt, name, id) " +
                        "ON ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES (null, 'null')");

        // Extra condition
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + POLYGONS_SQL + ") AS a (wkt, name, id) RIGHT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON a.name > b.name AND ST_Intersects(ST_GeometryFromText(b.wkt), ST_GeometryFromText(a.wkt))",
                "VALUES (null, 'c'), (null, 'd'), ('c', 'a'), ('c', 'b'), (null, 'empty'), (null, 'null')");
    }

    @Test
    public void testSpatialJoinOverRightJoin()
    {
        assertQuery("SELECT a.name, b.name, c.name " +
                        "FROM (" + POINTS_SQL + ") AS a (latitude, longitude, name, id) " +
                        "RIGHT JOIN (" + POINTS_SQL + ") AS b (latitude, longitude, name, id) ON a.latitude = b.latitude AND a.longitude = b.longitude AND a.latitude > 0 " +
                        "JOIN (" + POINTS_SQL + ") AS c (latitude, longitude, name, id) ON ST_Distance(ST_Point(a.latitude, b.longitude), ST_Point(c.latitude, c.longitude)) < 1 ",
                "VALUES ('y', 'y', 'y'), ('z', 'z', 'z')");
    }

    @Test
    public void testSpatialJoinOverLeftJoinWithOrPredicate()
    {
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "LEFT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b') , ('y', 'c'), ('z', NULL), ('null', NULL)");
    }

    @Test
    public void testSpatialJoinOverRightJoinWithOrPredicate()
    {
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "RIGHT JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b') , ('y', 'c'), (NULL, 'd'), (NULL, 'empty'), (NULL, 'null')");
    }

    @Test
    public void testSpatialJoinOverInnerJoinWithOrPredicate()
    {
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "INNER JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b') , ('y', 'c')");
    }

    @Test
    public void testSpatialJoinOverFullJoinWithOrPredicate()
    {
        assertQuery("SELECT a.name, b.name " +
                        "FROM (" + MULTI_POINTS_SQL + ") AS a (latitude1, longitude1, latitude2, longitude2, name, id) " +
                        "FULL JOIN (" + POLYGONS_SQL + ") AS b (wkt, name, id) " +
                        "ON ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude1, a.longitude1)) OR ST_Contains(ST_GeometryFromText(b.wkt), ST_Point(a.latitude2, a.longitude2))",
                "VALUES ('x', 'a'), ('y', 'b'), ('y', 'c'), (NULL, 'd'), (NULL, 'empty'), ('z', NULL), (NULL, 'null'), ('null', NULL)");
    }
}
