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
package io.trino.plugin.deltalake.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;

import java.util.Base64;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class DeltaLakeColumnStatistics
{
    private final OptionalLong totalSizeInBytes;
    private final HyperLogLog ndvSummary;

    @JsonCreator
    public static DeltaLakeColumnStatistics create(
            @JsonProperty("totalSizeInBytes") OptionalLong totalSizeInBytes,
            @JsonProperty("ndvSummary") String ndvSummaryBase64)
    {
        requireNonNull(totalSizeInBytes, "totalSizeInBytes is null");
        requireNonNull(ndvSummaryBase64, "ndvSummaryBase64 is null");
        byte[] ndvSummaryBytes = Base64.getDecoder().decode(ndvSummaryBase64);
        return new DeltaLakeColumnStatistics(totalSizeInBytes, HyperLogLog.newInstance(Slices.wrappedBuffer(ndvSummaryBytes)));
    }

    public static DeltaLakeColumnStatistics create(OptionalLong totalSizeInBytes, HyperLogLog ndvSummary)
    {
        return new DeltaLakeColumnStatistics(totalSizeInBytes, ndvSummary);
    }

    private DeltaLakeColumnStatistics(OptionalLong totalSizeInBytes, HyperLogLog ndvSummary)
    {
        this.totalSizeInBytes = requireNonNull(totalSizeInBytes, "totalSizeInBytes is null");
        this.ndvSummary = requireNonNull(ndvSummary, "ndvSummary is null");
    }

    @JsonProperty
    public OptionalLong getTotalSizeInBytes()
    {
        return totalSizeInBytes;
    }

    @JsonProperty("ndvSummary")
    public String getNdvSummaryBase64()
    {
        return Base64.getEncoder().encodeToString(ndvSummary.serialize().getBytes());
    }

    public HyperLogLog getNdvSummary()
    {
        return ndvSummary;
    }

    public DeltaLakeColumnStatistics update(DeltaLakeColumnStatistics newStatistics)
    {
        OptionalLong totalSizeInBytes = mergeIntegerStatistics(this.totalSizeInBytes, newStatistics.totalSizeInBytes);
        HyperLogLog ndvSummary = HyperLogLog.newInstance(this.ndvSummary.serialize());
        ndvSummary.mergeWith(newStatistics.ndvSummary);
        return new DeltaLakeColumnStatistics(totalSizeInBytes, ndvSummary);
    }

    private static OptionalLong mergeIntegerStatistics(OptionalLong first, OptionalLong second)
    {
        if (first.isPresent() && second.isPresent()) {
            return OptionalLong.of(first.getAsLong() + second.getAsLong());
        }
        return OptionalLong.empty();
    }
}
