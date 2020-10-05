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
package io.trino.plugin.datasketches.theta;

import io.airlift.slice.Slices;
import io.trino.plugin.datasketches.state.SketchState;
import io.trino.plugin.datasketches.state.SketchStateFactory;
import io.trino.spi.function.AccumulatorStateFactory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test funnel merging.
 */
public class MergeEstimateTest
{
    private SketchState state1;
    private SketchState state2;

    private static final long TEST_SEED = 95869L;
    private static final int TEST_ENTRIES = 2048;

    /**
     * Initialize state.
     */

    @BeforeClass
    public void init()
    {
        AccumulatorStateFactory<SketchState> factory = new SketchStateFactory();
        state1 = factory.createSingleState();
        state2 = factory.createSingleState();
    }

    /**
     * Simple sketch merge case.
     */
    @Test
    public void testSimpleMerge()
    {
        // 1000 unique keys
        UpdateSketch sketch1 = UpdateSketch.builder().setSeed(TEST_SEED).build();
        for (int key = 0; key < 1000; key++) {
            sketch1.update(key);
        }

        // 1000 unique keys
        // the first 500 unique keys overlap with sketch1
        UpdateSketch sketch2 = UpdateSketch.builder().setSeed(TEST_SEED).build();
        for (int key = 500; key < 1500; key++) {
            sketch2.update(key);
        }

        UnionWithParams.input(state1, Slices.wrappedBuffer(sketch1.compact().toByteArray()), TEST_ENTRIES, TEST_SEED);
        UnionWithParams.input(state2, Slices.wrappedBuffer(sketch2.compact().toByteArray()), TEST_ENTRIES, TEST_SEED);
        Union.combine(state1, state2);
        double estimate = Estimate.estimate(state1.getSketch(), TEST_SEED);

        org.apache.datasketches.theta.Union union = SetOperation.builder().setSeed(TEST_SEED).buildUnion();
        union.update(sketch1);
        union.update(sketch2);
        Sketch unionResult = union.getResult();

        assertEquals(unionResult.getEstimate(), estimate, 0);
    }
}
