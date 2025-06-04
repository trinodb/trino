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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.Resources.getResource;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.plugin.geospatial.BingTile.fromCoordinates;
import static io.trino.plugin.geospatial.BingTileType.BING_TILE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestBingTileFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new GeoPlugin());
        assertions.addFunctions(new InternalFunctionBundle(APPLY_FUNCTION));
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSerialization()
            throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapper();
        BingTile tile = fromCoordinates(1, 2, 3);
        String json = objectMapper.writeValueAsString(tile);
        assertThat("{\"x\":1,\"y\":2,\"zoom\":3}").isEqualTo(json);
        assertThat(tile).isEqualTo(objectMapper.readerFor(BingTile.class).readValue(json));
    }

    @Test
    public void testArrayOfBingTiles()
    {
        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "bing_tile(1, 2, 10)")
                .binding("b", "bing_tile(3, 4, 11)"))
                .hasType(new ArrayType(BING_TILE))
                .isEqualTo(ImmutableList.of(fromCoordinates(1, 2, 10), fromCoordinates(3, 4, 11)));
    }

    @Test
    public void testBingTile()
    {
        assertThat(assertions.function("bing_tile_quadkey", "bing_tile('213')"))
                .hasType(VARCHAR)
                .isEqualTo("213");

        assertThat(assertions.function("bing_tile_quadkey", "bing_tile('123030123010121')"))
                .hasType(VARCHAR)
                .isEqualTo("123030123010121");

        assertThat(assertions.function("bing_tile_quadkey", "bing_tile(3, 5, 3)"))
                .hasType(VARCHAR)
                .isEqualTo("213");

        assertThat(assertions.function("bing_tile_quadkey", "bing_tile(21845, 13506, 15)"))
                .hasType(VARCHAR)
                .isEqualTo("123030123010121");

        // Invalid calls: corrupt quadkeys
        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "''")::evaluate)
                .hasMessage("QuadKey must not be empty string");

        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "'test'")::evaluate)
                .hasMessage("Invalid QuadKey digit sequence: test");

        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "'12345'")::evaluate)
                .hasMessage("Invalid QuadKey digit sequence: 12345");

        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "'101010101010101010101010101010100101010101001010'")::evaluate)
                .hasMessage("QuadKey must be 23 characters or less");

        // Invalid calls: XY out of range
        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "10", "2", "3")::evaluate)
                .hasMessage("XY coordinates for a Bing tile at zoom level 3 must be within [0, 8) range");

        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "2", "10", "3")::evaluate)
                .hasMessage("XY coordinates for a Bing tile at zoom level 3 must be within [0, 8) range");

        // Invalid calls: zoom level out of range
        assertTrinoExceptionThrownBy(assertions.function("bing_tile", "2", "7", "37")::evaluate)
                .hasMessage("Zoom level must be <= 23");
    }

    @Test
    public void testPointToBingTile()
    {
        assertThat(assertions.function("bing_tile_at", "30.12", "60", "15"))
                .hasType(BING_TILE)
                .isEqualTo(fromCoordinates(21845, 13506, 15));

        assertThat(assertions.function("bing_tile_at", "0", "-0.002", "1"))
                .hasType(BING_TILE)
                .isEqualTo(fromCoordinates(0, 1, 1));

        assertThat(assertions.function("bing_tile_at", "1e0/512", "0", "1"))
                .hasType(BING_TILE)
                .isEqualTo(fromCoordinates(1, 0, 1));

        assertThat(assertions.function("bing_tile_at", "1e0/512", "0", "9"))
                .hasType(BING_TILE)
                .isEqualTo(fromCoordinates(256, 255, 9));

        // Invalid calls
        // Longitude out of range
        assertTrinoExceptionThrownBy(assertions.function("bing_tile_at", "30.12", "600", "15")::evaluate)
                .hasMessage("Longitude must be between -180.0 and 180.0");

        // Latitude out of range
        assertTrinoExceptionThrownBy(assertions.function("bing_tile_at", "300.12", "60", "15")::evaluate)
                .hasMessage("Latitude must be between -85.05112878 and 85.05112878");

        // Invalid zoom levels
        assertTrinoExceptionThrownBy(assertions.function("bing_tile_at", "30.12", "60", "0")::evaluate)
                .hasMessage("Zoom level must be > 0");

        assertTrinoExceptionThrownBy(assertions.function("bing_tile_at", "30.12", "60", "40")::evaluate)
                .hasMessage("Zoom level must be <= 23");
    }

    @Test
    public void testBingTileCoordinates()
    {
        assertThat(assertions.expression("bing_tile_coordinates(tile)[1]")
                .binding("tile", "bing_tile('213')"))
                .isEqualTo(3);

        assertThat(assertions.expression("bing_tile_coordinates(tile)[2]")
                .binding("tile", "bing_tile('213')"))
                .isEqualTo(5);

        assertThat(assertions.expression("bing_tile_coordinates(tile)[1]")
                .binding("tile", "bing_tile('123030123010121')"))
                .isEqualTo(21845);

        assertThat(assertions.expression("bing_tile_coordinates(tile)[2]")
                .binding("tile", "bing_tile('123030123010121')"))
                .isEqualTo(13506);
    }

    private void assertBingTilesAroundWithRadius(
            double latitude,
            double longitude,
            int zoomLevel,
            double radius,
            String... expectedQuadKeys)
    {
        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(%s, %s, %s, %s)".formatted(latitude, longitude, zoomLevel, radius)))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.copyOf(expectedQuadKeys));
    }

    @Test
    public void testBingTilesAroundWithRadius()
    {
        assertBingTilesAroundWithRadius(30.12, 60, 1, 1000, "1");

        assertBingTilesAroundWithRadius(30.12, 60, 15, .5,
                "123030123010120", "123030123010121", "123030123010123");

        assertBingTilesAroundWithRadius(30.12, 60, 19, .05,
                "1230301230101212120",
                "1230301230101212121",
                "1230301230101212130",
                "1230301230101212103",
                "1230301230101212123",
                "1230301230101212112",
                "1230301230101212102");
    }

    @Test
    public void testBingTilesAroundCornerWithRadius()
    {
        // Different zoom Level
        assertBingTilesAroundWithRadius(-85.05112878, -180, 1, 500,
                "3", "2");

        assertBingTilesAroundWithRadius(-85.05112878, -180, 5, 200,
                "33332",
                "33333",
                "22222",
                "22223",
                "22220",
                "22221",
                "33330",
                "33331");

        assertBingTilesAroundWithRadius(-85.05112878, -180, 15, .2,
                "333333333333332",
                "333333333333333",
                "222222222222222",
                "222222222222223",
                "222222222222220",
                "222222222222221",
                "333333333333330",
                "333333333333331");

        // Different Corners
        // Starting Corner 0,3
        assertBingTilesAroundWithRadius(-85.05112878, -180, 4, 500,
                "3323", "3332", "3333", "2222", "2223", "2232", "2220", "2221", "3330", "3331");

        assertBingTilesAroundWithRadius(-85.05112878, 180, 4, 500,
                "3323", "3332", "3333", "2222", "2223", "2232", "3331", "2221", "2220", "3330");

        assertBingTilesAroundWithRadius(85.05112878, -180, 4, 500,
                "1101", "1110", "1111", "0000", "0001", "0010", "0002", "0003", "1112", "1113");

        assertBingTilesAroundWithRadius(85.05112878, 180, 4, 500,
                "1101", "1110", "1111", "0000", "0001", "0010", "1113", "0003", "0002", "1112");
    }

    @Test
    public void testBingTilesAroundEdgeWithRadius()
    {
        // Different zoom Level
        assertBingTilesAroundWithRadius(-85.05112878, 0, 3, 300,
                "233", "322");

        assertBingTilesAroundWithRadius(-85.05112878, 0, 12, 1,
                "233333333332",
                "233333333333",
                "322222222222",
                "322222222223",
                "322222222220",
                "233333333331");

        // Different Edges
        // Starting Edge 2,3
        assertBingTilesAroundWithRadius(-85.05112878, 0, 4, 100,
                "2333", "3222");

        assertBingTilesAroundWithRadius(85.05112878, 0, 4, 100,
                "0111", "1000");

        assertBingTilesAroundWithRadius(0, 180, 4, 100,
                "3111", "2000", "1333", "0222");

        assertBingTilesAroundWithRadius(0, -180, 4, 100,
                "3111", "2000", "0222", "1333");
    }

    @Test
    public void testBingTilesWithRadiusBadInput()
    {
        // Invalid radius
        assertTrinoExceptionThrownBy(assertions.function("bing_tiles_around", "30.12", "60.0", "1", "-1")::evaluate)
                .hasMessage("Radius must be >= 0");

        assertTrinoExceptionThrownBy(assertions.function("bing_tiles_around", "30.12", "60.0", "1", "2000")::evaluate)
                .hasMessage("Radius must be <= 1,000 km");

        // Too many tiles
        assertTrinoExceptionThrownBy(assertions.function("bing_tiles_around", "30.12", "60.0", "20", "100")::evaluate)
                .hasMessage("The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: 36699364. Radius: 100.0 km. Zoom level: 20.");
    }

    @Test
    public void testBingTilesAround()
    {
        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(30.12, 60, 1)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("0", "2", "1", "3"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(30.12, 60, 15)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of(
                        "123030123010102",
                        "123030123010120",
                        "123030123010122",
                        "123030123010103",
                        "123030123010121",
                        "123030123010123",
                        "123030123010112",
                        "123030123010130",
                        "123030123010132"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(30.12, 60, 23)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of(
                        "12303012301012121210122",
                        "12303012301012121210300",
                        "12303012301012121210302",
                        "12303012301012121210123",
                        "12303012301012121210301",
                        "12303012301012121210303",
                        "12303012301012121210132",
                        "12303012301012121210310",
                        "12303012301012121210312"));
    }

    @Test
    public void testBingTilesAroundCorner()
    {
        // Different zoom Level
        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, -180, 1)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("0", "2", "1", "3"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, -180, 3)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("220", "222", "221", "223"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, -180, 15)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("222222222222220", "222222222222222", "222222222222221", "222222222222223"));

        // Different Corners
        // Starting Corner 0,3
        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, -180, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("20", "22", "21", "23"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, 180, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("30", "32", "31", "33"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(85.05112878, -180, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("00", "02", "01", "03"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(85.05112878, 180, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("10", "12", "11", "13"));
    }

    @Test
    public void testBingTilesAroundEdge()
    {
        // Different zoom Level
        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, 0, 1)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("0", "2", "1", "3"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, 0, 3)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("231", "233", "320", "322", "321", "323"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, 0, 15)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of(
                        "233333333333331",
                        "233333333333333",
                        "322222222222220",
                        "322222222222222",
                        "322222222222221",
                        "322222222222223"));

        // Different Edges
        // Starting Edge 2,3
        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(-85.05112878, 0, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("21", "23", "30", "32", "31", "33"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(85.05112878, 0, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("01", "03", "10", "12", "11", "13"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(0, 180, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("12", "30", "32", "13", "31", "33"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "bing_tiles_around(0, -180, 2)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("02", "20", "22", "03", "21", "23"));
    }

    @Test
    public void testBingTileZoomLevel()
    {
        assertThat(assertions.function("bing_tile_zoom_level", "bing_tile('213')"))
                .hasType(TINYINT)
                .isEqualTo((byte) 3);

        assertThat(assertions.function("bing_tile_zoom_level", "bing_tile('123030123010121')"))
                .hasType(TINYINT)
                .isEqualTo((byte) 15);
    }

    @Test
    public void testBingTilePolygon()
    {
        assertThat(assertions.function("ST_AsText", "bing_tile_polygon(bing_tile('123030123010121'))"))
                .hasType(VARCHAR)
                .isEqualTo("POLYGON ((59.996337890625 30.11662158281937, 60.00732421875 30.11662158281937, 60.00732421875 30.12612436422458, 59.996337890625 30.12612436422458, 59.996337890625 30.11662158281937))");

        assertThat(assertions.function("ST_AsText", "ST_Centroid(bing_tile_polygon(bing_tile('123030123010121')))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (60.0018310546875 30.121372973521975)");

        // Check bottom right corner of a stack of tiles at different zoom levels
        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(1, 1, 1)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (180 -85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(3, 3, 2)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (180 -85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(7, 7, 3)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (180 -85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(15, 15, 4)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (180 -85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(31, 31, 5)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (180 -85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(0, 0, 1)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(1, 1, 2)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(3, 3, 3)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(7, 7, 4)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(15, 15, 5)), g -> ST_Point(ST_XMax(g), ST_YMin(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        // Check top left corner of a stack of tiles at different zoom levels
        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(1, 1, 1)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(2, 2, 2)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(4, 4, 3)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(8, 8, 4)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(16, 16, 5)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (0 0)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(0, 0, 1)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (-180 85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(0, 0, 2)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (-180 85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(0, 0, 3)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (-180 85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(0, 0, 4)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (-180 85.05112877980659)");

        assertThat(assertions.function("ST_AsText", "apply(bing_tile_polygon(bing_tile(0, 0, 5)), g -> ST_Point(ST_XMin(g), ST_YMax(g)))"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (-180 85.05112877980659)");
    }

    @Test
    public void testLargeGeometryToBingTiles()
            throws Exception
    {
        Path filePath = new File(getResource("large_polygon.txt").toURI()).toPath();
        List<String> lines = Files.readAllLines(filePath);
        for (String line : lines) {
            String[] parts = line.split("\\|");
            String wkt = parts[0];
            int zoomLevel = Integer.parseInt(parts[1]);
            long tileCount = Long.parseLong(parts[2]);
            assertThat(assertions.expression("cardinality(geometry_to_bing_tiles(geometry, zoom))")
                    .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                    .binding("zoom", Integer.toString(zoomLevel)))
                    .isEqualTo(tileCount);
        }
    }

    @Test
    public void testGeometryToBingTiles()
            throws Exception
    {
        assertGeometryToBingTiles("POINT (60 30.12)", 10, ImmutableList.of("1230301230"));
        assertGeometryToBingTiles("POINT (60 30.12)", 15, ImmutableList.of("123030123010121"));
        assertGeometryToBingTiles("POINT (60 30.12)", 16, ImmutableList.of("1230301230101212"));

        assertGeometryToBingTiles("POLYGON ((0 0, 0 10, 10 10, 10 0))", 6, ImmutableList.of("122220", "122222", "122221", "122223"));
        assertGeometryToBingTiles("POLYGON ((0 0, 0 10, 10 10))", 6, ImmutableList.of("122220", "122222", "122221"));

        assertGeometryToBingTiles("POLYGON ((10 10, -10 10, -20 -15, 10 10))", 3, ImmutableList.of("033", "211", "122"));
        assertGeometryToBingTiles("POLYGON ((10 10, -10 10, -20 -15, 10 10))", 6, ImmutableList.of("211102", "211120", "033321", "033323", "211101", "211103", "211121", "033330", "033332", "211110", "211112", "033331", "033333", "211111", "122220", "122222", "122221"));

        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12))", 10, ImmutableList.of("1230301230"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12))", 15, ImmutableList.of("123030123010121"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POLYGON ((10 10, -10 10, -20 -15, 10 10)))", 3, ImmutableList.of("033", "211", "122"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12), POLYGON ((10 10, -10 10, -20 -15, 10 10)))", 3, ImmutableList.of("033", "211", "122", "123"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12), LINESTRING (61 31, 61.01 31.01), POLYGON EMPTY)", 15, ImmutableList.of("123030123010121", "123030112310200", "123030112310202", "123030112310201"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "geometry_to_bing_tiles(bing_tile_polygon(bing_tile('1230301230')), 10)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("1230301230"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "geometry_to_bing_tiles(bing_tile_polygon(bing_tile('1230301230')), 11)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("12303012300", "12303012302", "12303012301", "12303012303"));

        assertThat(assertions.expression("transform(tiles, x -> bing_tile_quadkey(x))")
                .binding("tiles", "geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('LINESTRING (59.765625 29.84064389983442, 60.2 30.14512718337612)')), 10)"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("1230301230", "1230301231"));

        // Empty geometries
        assertGeometryToBingTiles("POINT EMPTY", 10, emptyList());
        assertGeometryToBingTiles("POLYGON EMPTY", 10, emptyList());
        assertGeometryToBingTiles("GEOMETRYCOLLECTION EMPTY", 10, emptyList());

        // Invalid input
        // Longitude out of range
        assertTrinoExceptionThrownBy(() -> assertions.expression("geometry_to_bing_tiles(geometry, zoom)")
                .binding("geometry", "ST_Point(600, 30.12)")
                .binding("zoom", Integer.toString(10))
                .evaluate())
                .hasMessage("Longitude span for the geometry must be in [-180.00, 180.00] range");

        assertTrinoExceptionThrownBy(() -> assertions.expression("geometry_to_bing_tiles(geometry, zoom)")
                .binding("geometry", "ST_GeometryFromText('POLYGON ((1000 10, -10 10, -20 -15))')")
                .binding("zoom", Integer.toString(10))
                .evaluate())
                .hasMessage("Longitude span for the geometry must be in [-180.00, 180.00] range");

        // Latitude out of range
        assertTrinoExceptionThrownBy(() -> assertions.expression("geometry_to_bing_tiles(geometry, zoom)")
                .binding("geometry", "ST_Point(60, 300.12)")
                .binding("zoom", Integer.toString(10))
                .evaluate())
                .hasMessage("Latitude span for the geometry must be in [-85.05, 85.05] range");

        assertTrinoExceptionThrownBy(() -> assertions.expression("geometry_to_bing_tiles(geometry, zoom)")
                .binding("geometry", "ST_GeometryFromText('POLYGON ((10 1000, -10 10, -20 -15))')")
                .binding("zoom", Integer.toString(10))
                .evaluate())
                .hasMessage("Latitude span for the geometry must be in [-85.05, 85.05] range");

        // Invalid zoom levels
        assertTrinoExceptionThrownBy(() -> assertions.expression("geometry_to_bing_tiles(geometry, zoom)")
                .binding("geometry", "ST_Point(60, 30.12)")
                .binding("zoom", Integer.toString(0))
                .evaluate())
                .hasMessage("Zoom level must be > 0");

        assertTrinoExceptionThrownBy(() -> assertions.expression("geometry_to_bing_tiles(geometry, zoom)")
                .binding("geometry", "ST_Point(60, 30.12)")
                .binding("zoom", Integer.toString(40))
                .evaluate())
                .hasMessage("Zoom level must be <= 23");

        // Input rectangle too large
        assertTrinoExceptionThrownBy(assertions.function("geometry_to_bing_tiles", "ST_Envelope(ST_GeometryFromText('LINESTRING (0 0, 80 80)'))", "16")::evaluate)
                .hasMessage("The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: 370085804. Rectangle: xMin=0.00, yMin=0.00, xMax=80.00, yMax=80.00. Zoom level: 16.");

        assertThat(assertions.function("cardinality", "geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('LINESTRING (0 0, 80 80)')), 5)"))
                .isEqualTo(104L);

        // Input polygon too complex
        String filePath = new File(getResource("too_large_polygon.txt").toURI()).getPath();
        String largeWkt;
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            largeWkt = lines.collect(onlyElement());
        }
        assertTrinoExceptionThrownBy(assertions.expression("geometry_to_bing_tiles(ST_GeometryFromText('" + largeWkt + "'), 16)")::evaluate)
                .hasMessage("The zoom level is too high or the geometry is too complex to compute a set of covering Bing tiles. Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");

        assertThat(assertions.expression("cardinality(geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('" + largeWkt + "')), 16))"))
                .isEqualTo(19939L);

        // Zoom level is too high
        assertTrinoExceptionThrownBy(assertions.function("geometry_to_bing_tiles", "ST_GeometryFromText('POLYGON ((0 0, 0 20, 20 20, 0 0))')", "20")::evaluate)
                .hasMessage("The zoom level is too high to compute a set of covering Bing tiles.");

        assertThat(assertions.function("cardinality", "geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((0 0, 0 20, 20 20, 0 0))'), 14)"))
                .isEqualTo(428787L);
    }

    private void assertGeometryToBingTiles(String wkt, int zoomLevel, List<String> expectedQuadKeys)
    {
        assertThat(assertions.expression("transform(geometry_to_bing_tiles(geometry, zoom), x -> bing_tile_quadkey(x))")
                .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                .binding("zoom", Integer.toString(zoomLevel)))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(expectedQuadKeys);
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "bing_tile(3, 5, 3)", "bing_tile(3, 5, 3)"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "bing_tile('213')", "bing_tile(3, 5, 3)"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "bing_tile('213')", "bing_tile('213')"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "bing_tile(3, 5, 3)", "bing_tile(3, 5, 4)"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "bing_tile('213')", "bing_tile('2131')"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "bing_tile(3, 5, 3)")
                .binding("b", "bing_tile(3, 5, 3)"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "bing_tile('213')")
                .binding("b", "bing_tile(3, 5, 3)"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "bing_tile('213')")
                .binding("b", "bing_tile('213')"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "bing_tile(3, 5, 3)")
                .binding("b", "bing_tile(3, 5, 4)"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "bing_tile('213')")
                .binding("b", "bing_tile('2131')"))
                .isEqualTo(true);
    }

    @Test
    public void testIdentical()
    {
        assertThat(assertions.operator(IDENTICAL, "null", "null"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "bing_tile(3, 5, 3)", "null"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "null", "bing_tile(3, 5, 3)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "bing_tile(3, 5, 3)", "bing_tile(3, 5, 3)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "bing_tile('213')", "bing_tile(3, 5, 3)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "bing_tile('213')", "bing_tile('213')"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "bing_tile(3, 5, 3)", "bing_tile(3, 5, 4)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "bing_tile('213')", "bing_tile('2131')"))
                .isEqualTo(false);
    }
}
