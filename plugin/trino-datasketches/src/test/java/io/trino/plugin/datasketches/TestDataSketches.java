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
package io.trino.plugin.datasketches;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.plugin.datasketches.state.SketchState;
import io.trino.plugin.datasketches.state.SketchStateFactory;
import io.trino.plugin.datasketches.theta.Estimate;
import io.trino.plugin.datasketches.theta.SketchFunctionsPlugin;
import io.trino.plugin.datasketches.theta.Union;
import io.trino.plugin.datasketches.theta.UnionWithParams;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.datasketches.theta.ThetaSetOperation;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.SqlPath.buildPath;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_NOMINAL_ENTRIES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDataSketches
        extends AbstractTestQueryFramework
{
    private static final long TEST_SEED = 95869L;
    private static final int TEST_ENTRIES = 2048;
    private static final int DEFAULT_ENTRIES = DEFAULT_NOMINAL_ENTRIES;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setPath(buildPath("datasketches.theta", Optional.empty()))
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new SketchFunctionsPlugin());
        queryRunner.createCatalog("datasketches", "datasketches");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }

    @Test
    public void testSimpleMerge()
    {
        SketchStateFactory factory = new SketchStateFactory();
        SketchState state1 = factory.createSingleState();
        SketchState state2 = factory.createSingleState();

        // 1000 unique keys
        UpdatableThetaSketch sketch1 = UpdatableThetaSketch.builder()
                .setSeed(TEST_SEED)
                .setNominalEntries(TEST_ENTRIES)
                .build();
        for (int key = 0; key < 1000; key++) {
            sketch1.update(key);
        }

        // 1000 unique keys
        // the first 500 unique keys overlap with sketch1
        UpdatableThetaSketch sketch2 = UpdatableThetaSketch.builder()
                .setSeed(TEST_SEED)
                .setNominalEntries(TEST_ENTRIES)
                .build();
        for (int key = 500; key < 1500; key++) {
            sketch2.update(key);
        }

        UnionWithParams.input(state1, Slices.wrappedBuffer(sketch1.compact().toByteArray()), TEST_ENTRIES, TEST_SEED);
        UnionWithParams.input(state2, Slices.wrappedBuffer(sketch2.compact().toByteArray()), TEST_ENTRIES, TEST_SEED);
        Union.combine(state1, state2);
        double estimate = Estimate.estimate(state1.getSketch(), TEST_SEED);

        ThetaUnion union = ThetaSetOperation.builder().setSeed(TEST_SEED).setNominalEntries(TEST_ENTRIES).buildUnion();
        union.union(sketch1);
        union.union(sketch2);
        ThetaSketch unionResult = union.getResult();

        assertThat(estimate).isEqualTo(unionResult.getEstimate());
    }

    @Test
    public void testMergeAndEstimate()
    {
        int[] idACatagory = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] idBCatagory = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        String sketchA = toHexSketch(idACatagory);
        String sketchB = toHexSketch(idBCatagory);

        MaterializedResult actualEstimateResult = computeActual("""
                SELECT category, theta_sketch_estimate(sketch)
                FROM (VALUES
                        ('a', X'%s'),
                        ('b', X'%s')) t(category, sketch)
                ORDER BY category
                """.formatted(sketchA, sketchB));
        assertThat(actualEstimateResult.getRowCount()).isEqualTo(2);
        MaterializedResult expectedEstimateResult = resultBuilder(getSession(), VARCHAR, DOUBLE)
                .row("a", 10d)
                .row("b", 10d)
                .build();
        assertThat(actualEstimateResult.getMaterializedRows())
                .isEqualTo(expectedEstimateResult.getMaterializedRows());

        double mergeEstimateResult = (double) computeScalar("""
                SELECT theta_sketch_estimate(theta_sketch_union(sketch))
                FROM (VALUES
                        (X'%s'),
                        (X'%s')) t(sketch)
                """.formatted(sketchA, sketchB));
        assertThat(mergeEstimateResult).isEqualTo(15d);
    }

    @Test
    public void testMergeDoesNotDoubleCount()
    {
        SketchStateFactory factory = new SketchStateFactory();
        SketchState left = factory.createSingleState();
        left.setNominalEntries(DEFAULT_ENTRIES);
        left.setSeed(DEFAULT_UPDATE_SEED);
        left.addSketch(toSketchSlice(new int[] {1, 2, 3}, DEFAULT_UPDATE_SEED, DEFAULT_ENTRIES));

        SketchState right = factory.createSingleState();
        right.setNominalEntries(DEFAULT_ENTRIES);
        right.setSeed(DEFAULT_UPDATE_SEED);
        right.addSketch(toSketchSlice(new int[] {3, 4, 5}, DEFAULT_UPDATE_SEED, DEFAULT_ENTRIES));

        left.merge(right);

        double estimate = ThetaSketch.wrap(toMemory(left.getSketch()), DEFAULT_UPDATE_SEED).getEstimate();
        assertThat(estimate).isEqualTo(5d);
    }

    @Test
    public void testMergeWithEmptyStateDoesNotFail()
    {
        SketchStateFactory factory = new SketchStateFactory();
        SketchState empty = factory.createSingleState();
        empty.setNominalEntries(DEFAULT_ENTRIES);
        empty.setSeed(DEFAULT_UPDATE_SEED);

        SketchState populated = factory.createSingleState();
        populated.setNominalEntries(DEFAULT_ENTRIES);
        populated.setSeed(DEFAULT_UPDATE_SEED);
        populated.addSketch(toSketchSlice(new int[] {1, 2, 3, 4}, DEFAULT_UPDATE_SEED, DEFAULT_ENTRIES));

        empty.merge(populated);

        double estimate = ThetaSketch.wrap(toMemory(empty.getSketch()), DEFAULT_UPDATE_SEED).getEstimate();
        assertThat(estimate).isEqualTo(4d);
    }

    @Test
    public void testUnionIgnoresNullInputs()
    {
        String sketch = toHexSketch(new int[] {1, 2, 3});

        double estimate = (double) computeScalar("""
                SELECT theta_sketch_estimate(theta_sketch_union(sketch))
                FROM (VALUES
                        (CAST(NULL AS VARBINARY)),
                        (X'%s'),
                        (CAST(NULL AS VARBINARY))) t(sketch)
                """.formatted(sketch));

        assertThat(estimate).isEqualTo(3d);
    }

    private String toHexSketch(int[] data)
    {
        UpdatableThetaSketch sketch = UpdatableThetaSketch.builder()
                .setNominalEntries(DEFAULT_NOMINAL_ENTRIES)
                .setSeed(DEFAULT_UPDATE_SEED)
                .build();
        Arrays.stream(data).forEach(sketch::update);

        return base16().lowerCase().encode(sketch.compact().toByteArray());
    }

    private static Slice toSketchSlice(int[] data, long seed, int nominalEntries)
    {
        UpdatableThetaSketch sketch = UpdatableThetaSketch.builder()
                .setNominalEntries(nominalEntries)
                .setSeed(seed)
                .build();
        Arrays.stream(data).forEach(sketch::update);
        return Slices.wrappedBuffer(sketch.compact().toByteArray());
    }

    private static java.lang.foreign.MemorySegment toMemory(Slice slice)
    {
        return java.lang.foreign.MemorySegment.ofArray(slice.getBytes());
    }
}
