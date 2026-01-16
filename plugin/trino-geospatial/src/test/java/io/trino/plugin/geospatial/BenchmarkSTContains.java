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

import io.airlift.slice.Slices;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.trino.geospatial.serde.JtsGeometrySerde.deserialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeEnvelope;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSTContains
{
    @Benchmark
    public Object stContainsInnerPointSimpleGeometry(BenchmarkData data)
    {
        return GeoFunctions.stContains(data.simpleGeometry, data.innerPoint);
    }

    @Benchmark
    public Object stContainsOuterPointInEnvelopeSimpleGeometry(BenchmarkData data)
    {
        return GeoFunctions.stContains(data.simpleGeometry, data.outerPointInEnvelope);
    }

    @Benchmark
    public Object stContainsOuterPointNotInEnvelopeSimpleGeometry(BenchmarkData data)
    {
        return GeoFunctions.stContains(data.simpleGeometry, data.outerPointNotInEnvelope);
    }

    @Benchmark
    public Object deserializeSimpleGeometry(BenchmarkData data)
    {
        return deserialize(serialize(data.simpleGeometry));
    }

    @Benchmark
    public Object deserializeEnvelopeSimpleGeometry(BenchmarkData data)
    {
        return deserializeEnvelope(serialize(data.simpleGeometry));
    }

    @Benchmark
    public Object stContainsInnerPoint(BenchmarkData data)
    {
        return GeoFunctions.stContains(data.geometry, data.innerPoint);
    }

    @Benchmark
    public Object stContainsInnerPointDeserialized(BenchmarkData data)
    {
        return data.jtsGeometry.contains(data.innerJtsPoint);
    }

    @Benchmark
    public Object stContainsOuterPointInEnvelope(BenchmarkData data)
    {
        return GeoFunctions.stContains(data.geometry, data.outerPointInEnvelope);
    }

    @Benchmark
    public Object stContainsOuterPointInEnvelopeDeserialized(BenchmarkData data)
    {
        return data.jtsGeometry.contains(data.outerJtsPointInEnvelope);
    }

    @Benchmark
    public Object stContainsOuterPointNotInEnvelope(BenchmarkData data)
    {
        return GeoFunctions.stContains(data.geometry, data.outerPointNotInEnvelope);
    }

    @Benchmark
    public Object stContainsOuterPointNotInEnvelopeDeserialized(BenchmarkData data)
    {
        return data.jtsGeometry.contains(data.outerJtsPointNotInEnvelope);
    }

    @Benchmark
    public Object benchmarkDeserialize(BenchmarkData data)
    {
        return deserialize(serialize(data.geometry));
    }

    @Benchmark
    public Object benchmarkDeserializeEnvelope(BenchmarkData data)
    {
        return deserializeEnvelope(serialize(data.geometry));
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Geometry geometry;
        private Geometry simpleGeometry;
        private Geometry innerPoint;
        private Geometry outerPointInEnvelope;
        private Geometry outerPointNotInEnvelope;
        private Geometry jtsGeometry;
        private Point innerJtsPoint;
        private Point outerJtsPointInEnvelope;
        private Point outerJtsPointNotInEnvelope;

        @Setup
        public void setup()
                throws IOException
        {
            geometry = GeoFunctions.stGeometryFromText(Slices.utf8Slice(loadPolygon("large_polygon.txt")));
            simpleGeometry = GeoFunctions.stGeometryFromText(Slices.utf8Slice("POLYGON ((16.5 54, 16.5 54.1, 16.8 54.1, 16.8 54, 16.5 54))"));
            innerPoint = GeoFunctions.stPoint(16.6, 54.0167);
            outerPointInEnvelope = GeoFunctions.stPoint(16.6667, 54.05);
            outerPointNotInEnvelope = GeoFunctions.stPoint(16.6333, 54.2);

            jtsGeometry = geometry;
            innerJtsPoint = (Point) innerPoint;
            outerJtsPointInEnvelope = (Point) outerPointInEnvelope;
            outerJtsPointNotInEnvelope = (Point) outerPointNotInEnvelope;
        }
    }

    static void main()
            throws IOException, RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkSTContains benchmark = new BenchmarkSTContains();
        if (!((Boolean) benchmark.stContainsInnerPoint(data)).booleanValue()) {
            throw new IllegalStateException("ST_Contains for inner point expected to return true, got false.");
        }

        if (((Boolean) benchmark.stContainsOuterPointInEnvelope(data)).booleanValue()) {
            throw new IllegalStateException("ST_Contains for outer point expected to return false, got true.");
        }

        benchmark(BenchmarkSTContains.class).run();
    }
}
