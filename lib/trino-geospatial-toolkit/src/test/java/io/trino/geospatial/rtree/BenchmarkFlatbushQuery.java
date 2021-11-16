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
package io.trino.geospatial.rtree;

import io.trino.geospatial.Rectangle;
import io.trino.jmh.Benchmarks;
import org.locationtech.jts.index.ItemVisitor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.geospatial.rtree.RtreeTestUtils.makeRectangles;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFlatbushQuery
{
    private static final int SEED = 613;
    private static final int NUM_PROBE_RECTANGLES = 1000;

    @Benchmark
    @OperationsPerInvocation(NUM_PROBE_RECTANGLES)
    public void rtreeQuery(BenchmarkData data, Blackhole blackhole)
    {
        rtreeQueryProbe(data, blackhole::consume);
    }

    private void rtreeQueryProbe(BenchmarkData data, ItemVisitor visitor)
    {
        for (Rectangle query : data.getProbeRectangles()) {
            data.getRtree().findIntersections(query, visitor::visitItem);
        }
    }

    @Test
    public void testRtreeQuery()
    {
        for (int i = 0; i < 1000; i++) {
            TestVisitor visitor1 = new TestVisitor();
            BenchmarkData data = new BenchmarkData();
            data.setup();
            rtreeQueryProbe(data, visitor1::visit);

            TestVisitor visitor2 = new TestVisitor();
            Random random = new Random(SEED);
            List<Rectangle> probeRectangles = makeRectangles(random, NUM_PROBE_RECTANGLES);
            Flatbush<Rectangle> rtree = new Flatbush<>(makeRectangles(random, 1000).toArray(new Rectangle[] {}), 8);
            for (Rectangle query : probeRectangles) {
                rtree.findIntersections(query, visitor2::visit);
            }

            assertEquals(visitor1.getSize(), visitor2.getSize());
        }
    }

    public static class TestVisitor
    {
        private List<Rectangle> recs = new LinkedList<>();

        public void visit(Object obj)
        {
            assertEquals(obj.getClass(), Rectangle.class);
            Rectangle rec = (Rectangle) obj;
            assertFalse(rec.contains(110f, 110f));
            recs.add(rec);
        }

        public int getSize()
        {
            return recs.size();
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"8", "16", "32"})
        private int rtreeDegree = 8;
        @Param({"1000", "3000", "10000", "30000", "100000", "300000", "1000000"})
        private int numBuildRectangles = 1000;

        private List<Rectangle> probeRectangles;
        private Flatbush<Rectangle> rtree;

        @Setup
        public void setup()
        {
            Random random = new Random(SEED);
            probeRectangles = makeRectangles(random, NUM_PROBE_RECTANGLES);
            rtree = buildRtree(makeRectangles(random, numBuildRectangles));
        }

        public List<Rectangle> getProbeRectangles()
        {
            return probeRectangles;
        }

        public Flatbush<Rectangle> getRtree()
        {
            return rtree;
        }

        private Flatbush<Rectangle> buildRtree(List<Rectangle> rectangles)
        {
            return new Flatbush<>(rectangles.toArray(new Rectangle[] {}), rtreeDegree);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        new BenchmarkFlatbushQuery().testRtreeQuery();

        Benchmarks.benchmark(BenchmarkFlatbushQuery.class, WarmupMode.BULK).run();
    }
}
