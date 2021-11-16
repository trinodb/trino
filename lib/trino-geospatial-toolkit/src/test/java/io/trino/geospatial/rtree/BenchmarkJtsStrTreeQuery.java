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

import io.trino.jmh.Benchmarks;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.strtree.STRtree;
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
import java.util.stream.Collectors;

import static io.trino.geospatial.rtree.RtreeTestUtils.makeRectangles;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJtsStrTreeQuery
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
        for (Envelope query : data.getProbeEnvs()) {
            data.getRtree().query(query, visitor);
        }
    }

    @Test
    public void testRtreeQuery()
    {
        TestVisitor visitor1 = new TestVisitor();
        BenchmarkData data = new BenchmarkData();
        data.setup();
        rtreeQueryProbe(data, visitor1::visit);

        TestVisitor visitor2 = new TestVisitor();
        Random random = new Random(SEED);
        List<Envelope> probeEnvs = makeRectangles(random, NUM_PROBE_RECTANGLES).stream()
                .map(rect -> new Envelope(rect.getXMin(), rect.getXMax(), rect.getYMin(), rect.getYMax()))
                .collect(Collectors.toList());
        List<Envelope> buildEnvs = makeRectangles(random, 100).stream()
                .map(rect -> new Envelope(rect.getXMin(), rect.getXMax(), rect.getYMin(), rect.getYMax()))
                .collect(Collectors.toList());
        STRtree rtree = new STRtree();
        for (Envelope env : buildEnvs) {
            rtree.insert(env, env);
        }
        rtree.build();
        for (Envelope query : probeEnvs) {
            rtree.query(query, visitor2::visit);
        }

        assertEquals(visitor1.getSize(), visitor2.getSize());
    }

    public static class TestVisitor
    {
        private List<Envelope> envs = new LinkedList<>();

        public void visit(Object obj)
        {
            assertEquals(obj.getClass(), Envelope.class);
            Envelope env = (Envelope) obj;
            assertFalse(env.contains(100f, 100f));
            envs.add(env);
        }

        public int getSize()
        {
            return envs.size();
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1000", "3000", "10000", "30000", "100000", "300000", "1000000"})
        private int numBuildRectangles = 100;

        private List<Envelope> probeEnvs;
        private STRtree rtree;

        @Setup
        public void setup()
        {
            Random random = new Random(SEED);
            probeEnvs = makeRectangles(random, NUM_PROBE_RECTANGLES).stream()
                    .map(rect -> new Envelope(rect.getXMin(), rect.getXMax(), rect.getYMin(), rect.getYMax()))
                    .collect(Collectors.toList());
            List<Envelope> buildEnvs = makeRectangles(random, numBuildRectangles).stream()
                    .map(rect -> new Envelope(rect.getXMin(), rect.getXMax(), rect.getYMin(), rect.getYMax()))
                    .collect(Collectors.toList());
            rtree = buildRtree(buildEnvs);
        }

        public List<Envelope> getProbeEnvs()
        {
            return probeEnvs;
        }

        public STRtree getRtree()
        {
            return rtree;
        }

        private STRtree buildRtree(List<Envelope> envs)
        {
            STRtree rtree = new STRtree();
            for (Envelope env : envs) {
                rtree.insert(env, env);
            }
            rtree.build();
            return rtree;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        new BenchmarkJtsStrTreeQuery().testRtreeQuery();

        Benchmarks.benchmark(BenchmarkJtsStrTreeQuery.class, WarmupMode.BULK).run();
    }
}
