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
package io.trino.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.Slice;
import io.trino.block.BlockAssertions;
import io.trino.geospatial.serde.GeometrySerde;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.spi.Page;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;

public abstract class AbstractTestGeoAggregationFunctions
{
    private LocalQueryRunner runner;
    private TestingFunctionResolution functionResolution;

    @BeforeClass
    public final void initTestFunctions()
    {
        runner = LocalQueryRunner.builder(TEST_SESSION).build();
        runner.installPlugin(new GeoPlugin());
        functionResolution = new TestingFunctionResolution(runner);
    }

    @AfterClass(alwaysRun = true)
    public final void destroyTestFunctions()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    protected void assertAggregatedGeometries(String testDescription, String expectedWkt, String... wkts)
    {
        List<Slice> geometrySlices = Arrays.stream(wkts)
                .map(text -> text == null ? null : OGCGeometry.fromText(text))
                .map(input -> input == null ? null : GeometrySerde.serialize(input))
                .collect(Collectors.toList());

        // Add a custom equality assertion because the resulting geometry may have
        // its constituent points in a different order
        BiFunction<Object, Object, Boolean> equalityFunction = (left, right) -> {
            if (left == null && right == null) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            OGCGeometry leftGeometry = OGCGeometry.fromText(left.toString());
            OGCGeometry rightGeometry = OGCGeometry.fromText(right.toString());
            // Check for equality by getting the difference
            return leftGeometry.difference(rightGeometry).isEmpty() &&
                    rightGeometry.difference(leftGeometry).isEmpty();
        };
        // Test in forward and reverse order to verify that ordering doesn't affect the output
        assertAggregation(
                functionResolution,
                QualifiedName.of(getFunctionName()),
                fromTypes(GEOMETRY),
                equalityFunction,
                testDescription,
                new Page(BlockAssertions.createSlicesBlock(geometrySlices)),
                expectedWkt);
        Collections.reverse(geometrySlices);
        assertAggregation(
                functionResolution,
                QualifiedName.of(getFunctionName()),
                fromTypes(GEOMETRY),
                equalityFunction,
                testDescription,
                new Page(BlockAssertions.createSlicesBlock(geometrySlices)),
                expectedWkt);
    }

    protected abstract String getFunctionName();
}
