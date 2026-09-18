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
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.trino.block.BlockAssertions;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.math.DoubleMath.roundToInt;
import static io.trino.geospatial.KdbTree.buildKdbTree;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static io.trino.operator.aggregation.AggregationTestUtils.createGroupByIdBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.getGroupValue;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.math.RoundingMode.CEILING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpatialPartitioningInternalAggregation
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    public void test()
    {
        test(10);
        test(100);
    }

    public void test(int partitionCount)
    {
        QueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().build());
        runner.installPlugin(new GeoPlugin());

        TestingAggregationFunction function = new TestingFunctionResolution(runner)
                .getAggregateFunction("spatial_partitioning", fromTypes(GEOMETRY, INTEGER));

        List<Point> geometries = makeGeometries();
        String aggregation = aggregate(function, geometries, partitionCount);

        Rectangle expectedExtent = new Rectangle(-10, -10, Math.nextUp(10.0), Math.nextUp(10.0));
        Slice expectedValue = getSpatialPartitioning(expectedExtent, geometries, partitionCount);
        assertThat(aggregation).isEqualTo(expectedValue.toStringUtf8());

        String groupedAggregation = groupedAggregate(function, geometries, partitionCount);
        assertThat(groupedAggregation).isEqualTo(expectedValue.toStringUtf8());
    }

    @Test
    public void testUsesPlanarEnvelopeForSridAndZ()
    {
        QueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().build());
        runner.installPlugin(new GeoPlugin());

        TestingAggregationFunction function = new TestingFunctionResolution(runner)
                .getAggregateFunction("spatial_partitioning", fromTypes(GEOMETRY, INTEGER));

        List<Point> twoDimensionalGeometries = ImmutableList.of(
                point(0, 0),
                point(10, 5),
                point(5, 10));
        List<Point> geometriesWithMetadata = ImmutableList.of(
                pointWithSridAndZ(0, 0, 100),
                pointWithSridAndZ(10, 5, -100),
                pointWithSridAndZ(5, 10, 50));

        String aggregation = aggregate(function, geometriesWithMetadata, 2);

        Rectangle expectedExtent = new Rectangle(0, 0, Math.nextUp(10.0), Math.nextUp(10.0));
        Slice expectedValue = getSpatialPartitioning(expectedExtent, twoDimensionalGeometries, 2);
        assertThat(aggregation).isEqualTo(expectedValue.toStringUtf8());
    }

    private String aggregate(TestingAggregationFunction function, List<Point> geometries, int partitionCount)
    {
        Block geometryBlock = makeGeometryBlock(geometries);

        BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(1);
        INTEGER.writeInt(blockBuilder, partitionCount);
        Block partitionCountBlock = RunLengthEncodedBlock.create(blockBuilder.build(), geometries.size());

        AggregatorFactory aggregatorFactory = function.createAggregatorFactory(SINGLE, Ints.asList(0, 1), OptionalInt.empty());
        Page page = new Page(geometryBlock, partitionCountBlock);

        Aggregator aggregator = aggregatorFactory.createAggregator(new AggregationMetrics());
        aggregator.processPage(page);
        return (String) BlockAssertions.getOnlyValue(function.getFinalType(), getFinalBlock(function.getFinalType(), aggregator));
    }

    private String groupedAggregate(TestingAggregationFunction function, List<Point> geometries, int partitionCount)
    {
        Block geometryBlock = makeGeometryBlock(geometries);

        BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(1);
        INTEGER.writeInt(blockBuilder, partitionCount);
        Block partitionCountBlock = RunLengthEncodedBlock.create(blockBuilder.build(), geometries.size());

        AggregatorFactory aggregatorFactory = function.createAggregatorFactory(SINGLE, Ints.asList(0, 1), OptionalInt.empty());
        Page page = new Page(geometryBlock, partitionCountBlock);

        GroupedAggregator groupedAggregator = aggregatorFactory.createGroupedAggregator(new AggregationMetrics());
        groupedAggregator.processPage(0, createGroupByIdBlock(0, page.getPositionCount()), page);
        return (String) getGroupValue(function.getFinalType(), groupedAggregator, 0);
    }

    private List<Point> makeGeometries()
    {
        ImmutableList.Builder<Point> geometries = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                geometries.add(GEOMETRY_FACTORY.createPoint(new Coordinate(-10 + i, -10 + j)));
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                geometries.add(GEOMETRY_FACTORY.createPoint(new Coordinate(-10 + 2 * i, 2 * j)));
            }
        }

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                geometries.add(GEOMETRY_FACTORY.createPoint(new Coordinate(2.5 * i, -10 + 2.5 * j)));
            }
        }

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                geometries.add(GEOMETRY_FACTORY.createPoint(new Coordinate(5 * i, 5 * j)));
            }
        }

        return geometries.build();
    }

    private Point point(double x, double y)
    {
        return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    }

    private Point pointWithSridAndZ(double x, double y, double z)
    {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y, z));
        point.setSRID(4326);
        return point;
    }

    private Block makeGeometryBlock(List<Point> geometries)
    {
        BlockBuilder builder = GEOMETRY.createBlockBuilder(null, geometries.size());
        for (Geometry geometry : geometries) {
            GEOMETRY.writeSlice(builder, serialize(geometry));
        }
        return builder.build();
    }

    private Slice getSpatialPartitioning(Rectangle extent, List<Point> geometries, int partitionCount)
    {
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (Point geometry : geometries) {
            Envelope envelope = geometry.getEnvelopeInternal();
            rectangles.add(new Rectangle(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()));
        }

        return KdbTreeUtils.toJson(buildKdbTree(roundToInt(geometries.size() * 1.0 / partitionCount, CEILING), extent, rectangles.build()));
    }
}
