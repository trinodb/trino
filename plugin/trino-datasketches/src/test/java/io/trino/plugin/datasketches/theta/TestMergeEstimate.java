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
import io.trino.Session;
import io.trino.plugin.datasketches.state.SketchState;
import io.trino.plugin.datasketches.state.SketchStateFactory;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;

import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.testng.Assert.assertEquals;

public class TestMergeEstimate
        extends AbstractTestQueryFramework
{
    private SketchState state1;
    private SketchState state2;

    private static final long TEST_SEED = 95869L;
    private static final int TEST_ENTRIES = 2048;

    @BeforeClass
    public void setup()
    {
        AccumulatorStateFactory<SketchState> factory = new SketchStateFactory();
        state1 = factory.createSingleState();
        state2 = factory.createSingleState();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new SketchFunctionsPlugin());

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

    /**
     * Test case similar to theta sketch hive documented test at https://datasketches.apache.org/docs/Theta/ThetaHiveUDFs.html
     */
    @Test
    public void testMergeAndEstimate()
    {
        int[] idACatagory = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] idBCatagory = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        assertUpdate("CREATE TABLE sketch_intermediate (category VARCHAR, sketch VARBINARY)");
        assertUpdate("INSERT INTO sketch_intermediate VALUES ('a', X'" + getBytesWritableSketchUnion(idACatagory) + "')", 1);
        assertUpdate("INSERT INTO sketch_intermediate VALUES ('b', X'" + getBytesWritableSketchUnion(idBCatagory) + "')", 1);

        MaterializedResult actualEstimateResult = computeActual("SELECT category, theta_sketch_estimate(sketch) FROM sketch_intermediate ORDER BY category ASC");
        assertEquals(actualEstimateResult.getRowCount(), 2);
        MaterializedResult expectedEstimateResult = resultBuilder(getSession(), VARCHAR, DOUBLE)
                .row("a", 10d)
                .row("b", 10d)
                .build();
        assertEquals(actualEstimateResult.getMaterializedRows(), expectedEstimateResult.getMaterializedRows());

        double mergeEstimateResult = (double) computeScalar("SELECT theta_sketch_estimate(theta_sketch_union(sketch)) FROM sketch_intermediate");
        assertEquals(mergeEstimateResult, 15d);
    }

    private BytesWritable getBytesWritableSketchUnion(int[] data)
    {
        org.apache.datasketches.theta.Union union =
                SetOperation.builder().setSeed(DEFAULT_UPDATE_SEED).setNominalEntries(DEFAULT_NOMINAL_ENTRIES).buildUnion();
        Arrays.stream(data).forEach(e -> union.update(e));

        byte[] resultSketch = union.getResult().toByteArray();
        BytesWritable resultBytes = new BytesWritable();
        resultBytes.set(resultSketch, 0, resultSketch.length);
        return resultBytes;
    }
}
