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

import io.airlift.slice.Slice;
import io.trino.block.BlockAssertions;
import io.trino.geospatial.serde.JtsGeometrySerde;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.spi.Page;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

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
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractTestGeoAggregationFunctions
{
    private QueryRunner runner;
    private TestingFunctionResolution functionResolution;

    @BeforeAll
    public final void initTestFunctions()
    {
        runner = new StandaloneQueryRunner(TEST_SESSION);
        runner.installPlugin(new GeoPlugin());
        functionResolution = new TestingFunctionResolution(runner);
    }

    @AfterAll
    public final void destroyTestFunctions()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    protected void assertAggregatedGeometries(String testDescription, String expectedWkt, String... wkts)
    {
        WKTReader wktReader = new WKTReader();
        List<Slice> geometrySlices = Arrays.stream(wkts)
                .map(text -> {
                    if (text == null) {
                        return null;
                    }
                    try {
                        return JtsGeometrySerde.serialize(wktReader.read(text));
                    }
                    catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                })
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
            try {
                Geometry leftGeometry = wktReader.read(left.toString());
                Geometry rightGeometry = wktReader.read(right.toString());
                if (leftGeometry.isEmpty() && rightGeometry.isEmpty()) {
                    return leftGeometry.getGeometryType().equals(rightGeometry.getGeometryType());
                }
                return leftGeometry.equalsTopo(rightGeometry);
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
        };
        // Test in forward and reverse order to verify that ordering doesn't affect the output
        assertAggregation(
                functionResolution,
                getFunctionName(),
                fromTypes(GEOMETRY),
                equalityFunction,
                testDescription,
                new Page(BlockAssertions.createSlicesBlock(geometrySlices)),
                expectedWkt);
        Collections.reverse(geometrySlices);
        assertAggregation(
                functionResolution,
                getFunctionName(),
                fromTypes(GEOMETRY),
                equalityFunction,
                testDescription,
                new Page(BlockAssertions.createSlicesBlock(geometrySlices)),
                expectedWkt);
    }

    protected abstract String getFunctionName();
}
