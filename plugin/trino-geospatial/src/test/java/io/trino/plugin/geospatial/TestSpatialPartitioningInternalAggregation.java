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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.block.BlockAssertions;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.math.DoubleMath.roundToInt;
import static io.trino.geospatial.KdbTree.buildKdbTree;
import static io.trino.geospatial.serde.GeometrySerde.serialize;
import static io.trino.operator.aggregation.AggregationTestUtils.createGroupByIdBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.getGroupValue;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.math.RoundingMode.CEILING;
import static org.testng.Assert.assertEquals;

public class TestSpatialPartitioningInternalAggregation
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setup()
    {
        functionAssertions.installPlugin(new GeoPlugin());
    }

    @DataProvider(name = "partitionCount")
    public static Object[][] partitionCountProvider()
    {
        return new Object[][] {{100}, {10}};
    }

    @Test(dataProvider = "partitionCount")
    public void test(int partitionCount)
    {
        TestingAggregationFunction function = getFunction();
        List<OGCGeometry> geometries = makeGeometries();
        Block geometryBlock = makeGeometryBlock(geometries);

        Block partitionCountBlock = BlockAssertions.createRLEBlock(partitionCount, geometries.size());

        String expectedValue = getSpatialPartitioning(geometries, partitionCount);

        AggregatorFactory aggregatorFactory = function.createAggregatorFactory(SINGLE, Ints.asList(0, 1), OptionalInt.empty());
        Page page = new Page(geometryBlock, partitionCountBlock);

        Aggregator aggregator = aggregatorFactory.createAggregator();
        aggregator.processPage(page);
        String aggregation = (String) BlockAssertions.getOnlyValue(function.getFinalType(), getFinalBlock(function.getFinalType(), aggregator));
        assertEquals(aggregation, expectedValue);

        GroupedAggregator groupedAggregator = aggregatorFactory.createGroupedAggregator();
        groupedAggregator.processPage(createGroupByIdBlock(0, page.getPositionCount()), page);
        String groupValue = (String) getGroupValue(function.getFinalType(), groupedAggregator, 0);
        assertEquals(groupValue, expectedValue);
    }

    private TestingAggregationFunction getFunction()
    {
        return functionAssertions.getFunctionResolution().getAggregateFunction(
                QualifiedName.of("spatial_partitioning"),
                fromTypes(GEOMETRY, INTEGER));
    }

    private List<OGCGeometry> makeGeometries()
    {
        ImmutableList.Builder<OGCGeometry> geometries = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                geometries.add(new OGCPoint(new Point(-10 + i, -10 + j), null));
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                geometries.add(new OGCPoint(new Point(-10 + 2 * i, 2 * j), null));
            }
        }

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                geometries.add(new OGCPoint(new Point(2.5 * i, -10 + 2.5 * j), null));
            }
        }

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                geometries.add(new OGCPoint(new Point(5 * i, 5 * j), null));
            }
        }

        return geometries.build();
    }

    private Block makeGeometryBlock(List<OGCGeometry> geometries)
    {
        BlockBuilder builder = GEOMETRY.createBlockBuilder(null, geometries.size());
        for (OGCGeometry geometry : geometries) {
            GEOMETRY.writeSlice(builder, serialize(geometry));
        }
        return builder.build();
    }

    private String getSpatialPartitioning(List<OGCGeometry> geometries, int partitionCount)
    {
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (OGCGeometry geometry : geometries) {
            Envelope envelope = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(envelope);
            rectangles.add(new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
        }

        return KdbTreeUtils.toJson(buildKdbTree(roundToInt(geometries.size() * 1.0 / partitionCount, CEILING), rectangles.build()));
    }
}
