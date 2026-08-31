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
package io.trino.plugin.paimon;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.paimon.table.source.Split;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record PaimonSplit(String splitSerialized, Double weight, Long rowCount)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(PaimonSplit.class);

    public PaimonSplit(
            @JsonProperty(value = "splitSerialized", required = true) String splitSerialized,
            @JsonProperty(value = "weight", required = true) Double weight,
            @JsonProperty("rowCount") Long rowCount)
    {
        this.splitSerialized = requireNonNull(splitSerialized, "splitSerialized is null");
        checkArgument(!this.splitSerialized.isBlank(), "splitSerialized is blank");
        this.weight = requireNonNull(weight, "weight is null");
        checkArgument(Double.isFinite(weight) && weight > 0 && weight <= 1, "weight must be in the range (0, 1]");
        this.rowCount = rowCount;
        checkArgument(rowCount == null || rowCount >= 0, "rowCount must be non-negative");
    }

    public PaimonSplit(String splitSerialized, Double weight)
    {
        this(splitSerialized, weight, null);
    }

    @JsonCreator
    public static PaimonSplit fromJson(
            @JsonProperty(value = "splitSerialized", required = true) String splitSerialized,
            @JsonProperty(value = "weight", required = true) Double weight,
            @JsonProperty("rowCount") Long rowCount)
    {
        PaimonSplit split = new PaimonSplit(splitSerialized, weight, rowCount);
        decodeSerializedSplit(split.splitSerialized());
        return split;
    }

    @JsonAnySetter
    public void rejectUnknownJsonField(String name, Object value)
    {
        PaimonHandleJsonUtils.rejectUnknownHandleJsonField("PaimonSplit", name, value);
    }

    public static PaimonSplit fromSplit(Split split, Double weight)
    {
        requireNonNull(split, "split is null");
        return fromSplit(split, weight, PaimonSplitManager.splitWeightRowCount(split));
    }

    static PaimonSplit fromSplit(Split split, Double weight, long rowCount)
    {
        requireNonNull(split, "split is null");
        return new PaimonSplit(EncodingUtils.encodeObjectToString(split), weight, rowCount);
    }

    public Split decodeSplit()
    {
        return decodeSerializedSplit(splitSerialized);
    }

    private static Split decodeSerializedSplit(String splitSerialized)
    {
        Object decoded;
        try {
            decoded = EncodingUtils.decodeStringToObject(splitSerialized);
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("splitSerialized must contain a serialized Paimon Split", e);
        }
        checkArgument(decoded instanceof Split, "splitSerialized must contain a serialized Paimon Split");
        return (Split) decoded;
    }

    @Override
    @JsonProperty
    public String splitSerialized()
    {
        return splitSerialized;
    }

    @Override
    @JsonProperty
    public Double weight()
    {
        return weight;
    }

    @Override
    @JsonProperty
    public Long rowCount()
    {
        return rowCount;
    }

    @Override
    @JsonIgnore
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonIgnore
    public List<HostAddress> getAddresses()
    {
        return Collections.emptyList();
    }

    @Override
    @JsonIgnore
    public SplitWeight getSplitWeight()
    {
        return SplitWeight.fromProportion(weight);
    }

    @Override
    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(splitSerialized);
    }
}
