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
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.trino.geospatial.rtree.RtreeTestUtils.makeRectangles;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJtsStrTreeBuild
{
    private static final int SEED = 613;

    @Benchmark
    public STRtree buildRtree(BenchmarkData data)
    {
        STRtree rtree = new STRtree();
        for (Envelope envelope : data.getBuildEnvelopes()) {
            rtree.insert(envelope, envelope);
        }
        rtree.build();
        return rtree;
    }

    @Test
    public void testBuildRtree()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        STRtree benchmarkResult = buildRtree(data);

        STRtree rtree = new STRtree();
        List<Envelope> buildEnvelopes = makeRectangles(new Random(SEED), 100).stream()
                .map(rectangle -> new Envelope(rectangle.getXMin(), rectangle.getXMax(), rectangle.getYMin(), rectangle.getYMax()))
                .collect(Collectors.toList());
        for (Envelope envelope : buildEnvelopes) {
            rtree.insert(envelope, envelope);
        }
        rtree.build();

        assertEquals(benchmarkResult.size(), rtree.size());
        assertEquals(benchmarkResult.depth(), rtree.depth());
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1000", "3000", "10000", "30000", "100000", "300000", "1000000"})
        private int numBuildEnvelopes = 100;

        private List<Envelope> buildEnvelopes;

        @Setup
        public void setup()
        {
            buildEnvelopes = makeRectangles(new Random(SEED), numBuildEnvelopes).stream()
                    .map(rectangle -> new Envelope(rectangle.getXMin(), rectangle.getXMax(), rectangle.getYMin(), rectangle.getYMax()))
                    .collect(Collectors.toList());
        }

        public List<Envelope> getBuildEnvelopes()
        {
            return buildEnvelopes;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        new BenchmarkJtsStrTreeBuild().testBuildRtree();

        Benchmarks.benchmark(BenchmarkJtsStrTreeBuild.class, WarmupMode.BULK).run();
    }
}
