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
package io.trino.plugin.iceberg.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;

import java.nio.ByteBuffer;

public record PartitionFieldSummaryBean(
        boolean containsNull,
        ByteBuffer lowerBound,
        ByteBuffer upperBound)
        implements PartitionFieldSummary
{
    @JsonCreator
    public PartitionFieldSummaryBean(
            @JsonProperty("containsNull") boolean containsNull,
            @JsonProperty("lowerBound") ByteBuffer lowerBound,
            @JsonProperty("upperBound") ByteBuffer upperBound)
    {
        this.containsNull = containsNull;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @JsonProperty
    @Override
    public boolean containsNull()
    {
        return containsNull;
    }

    @JsonProperty
    @Override
    public ByteBuffer lowerBound()
    {
        return lowerBound;
    }

    @JsonProperty
    @Override
    public ByteBuffer upperBound()
    {
        return upperBound;
    }

    @Override
    public PartitionFieldSummary copy()
    {
        throw new UnsupportedOperationException("Cannot copy");
    }

    public static PartitionFieldSummaryBean from(PartitionFieldSummary partitionFieldSummary)
    {
        return new PartitionFieldSummaryBean(partitionFieldSummary.containsNull(), partitionFieldSummary.lowerBound(), partitionFieldSummary.upperBound());
    }
}
