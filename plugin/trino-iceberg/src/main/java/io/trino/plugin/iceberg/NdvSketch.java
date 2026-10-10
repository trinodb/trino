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
package io.trino.plugin.iceberg;

import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import org.apache.datasketches.theta.CompactThetaSketch;
import org.apache.datasketches.theta.ThetaSetOperation;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;

public sealed interface NdvSketch
        permits NdvSketch.Theta, NdvSketch.Hll
{
    String TRINO_DATASKETCHES_HLL_V1 = "trino-datasketches-hll-v1";

    long estimate();

    byte[] serialize();

    String blobType();

    NdvSketch mergeWith(NdvSketch other);

    static NdvSketch deserialize(String blobType, ByteBuffer buffer)
    {
        if (blobType.equals(APACHE_DATASKETCHES_THETA_V1)) {
            return new Theta(CompactThetaSketch.wrap(MemorySegment.ofBuffer(buffer)));
        }
        if (blobType.equals(TRINO_DATASKETCHES_HLL_V1)) {
            return new Hll(HyperLogLog.newInstance(Slices.wrappedHeapBuffer(buffer)));
        }
        throw new IllegalArgumentException("Unrecognized NDV sketch blob type: " + blobType);
    }

    record Theta(CompactThetaSketch sketch)
            implements NdvSketch
    {
        public Theta
        {
            requireNonNull(sketch, "sketch is null");
        }

        @Override
        public long estimate()
        {
            return (long) sketch.getEstimate();
        }

        @Override
        public byte[] serialize()
        {
            return sketch.toByteArray();
        }

        @Override
        public String blobType()
        {
            return APACHE_DATASKETCHES_THETA_V1;
        }

        @Override
        public NdvSketch mergeWith(NdvSketch other)
        {
            checkArgument(other instanceof Theta, "Cannot merge a Theta sketch with %s", other);
            CompactThetaSketch otherSketch = ((Theta) other).sketch();
            return new Theta(ThetaSetOperation.builder().buildUnion().union(sketch, otherSketch));
        }
    }

    record Hll(HyperLogLog sketch)
            implements NdvSketch
    {
        public Hll
        {
            requireNonNull(sketch, "sketch is null");
        }

        @Override
        public long estimate()
        {
            return sketch.cardinality();
        }

        @Override
        public byte[] serialize()
        {
            return sketch.serialize().getBytes();
        }

        @Override
        public String blobType()
        {
            return TRINO_DATASKETCHES_HLL_V1;
        }

        @Override
        public NdvSketch mergeWith(NdvSketch other)
        {
            checkArgument(other instanceof Hll, "Cannot merge an HLL sketch with %s", other);
            // HyperLogLog.mergeWith mutates the receiver in place, so merge into a fresh copy
            // rather than one of the two input sketches, which callers may still reference.
            HyperLogLog merged = HyperLogLog.newInstance(sketch.serialize());
            merged.mergeWith(((Hll) other).sketch());
            return new Hll(merged);
        }
    }
}
