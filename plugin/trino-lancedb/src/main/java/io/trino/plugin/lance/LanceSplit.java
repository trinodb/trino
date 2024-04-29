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
package io.trino.plugin.lance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LanceSplit
        implements ConnectorSplit
{
    private static final Joiner JOINER = Joiner.on(",");
    private static final int INSTANCE_SIZE = instanceSize(LanceSplit.class);

    private final SplitType splitType;
    private final Optional<String> suffix;
    private final List<String> segments;

    @JsonCreator
    public LanceSplit(
            @JsonProperty("splitType") SplitType splitType,
            @JsonProperty("suffix") Optional<String> suffix,
            @JsonProperty("fragments") List<String> fragments)
    {
        this.splitType = requireNonNull(splitType, "splitType id is null");
        this.suffix = requireNonNull(suffix, "suffix is null");
        this.segments = ImmutableList.copyOf(requireNonNull(fragments, "fragments is null"));

        // make sure the segment properties are present when the split type is segment
        if (splitType == SplitType.FRAGMENT) {
            checkArgument(suffix.isPresent(), "Suffix is missing from this split");
            checkArgument(!fragments.isEmpty(), "Segments are missing from the split");
        }
    }

    public static LanceSplit createHTTPSplit()
    {
        return new LanceSplit(
                SplitType.HTTP,
                Optional.empty(),
                ImmutableList.of());
    }

    public static LanceSplit createFragmentSplit(String suffix, List<String> fragments)
    {
        return new LanceSplit(
                SplitType.FRAGMENT,
                Optional.of(requireNonNull(suffix, "suffix is null")),
                requireNonNull(fragments, "fragments are null"));
    }

    @JsonProperty
    public SplitType getSplitType()
    {
        return splitType;
    }

    @JsonProperty
    public Optional<String> getSuffix()
    {
        return suffix;
    }

    @JsonProperty
    public List<String> getSegments()
    {
        return segments;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitType", splitType)
                .add("segments", segments)
                .toString();
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of(
                "splitType", splitType.name(),
                "suffix", suffix.orElse(""),
                "segments", JOINER.join(segments));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(suffix, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(segments, SizeOf::estimatedSizeOf);
    }

    public enum SplitType
    {
        FRAGMENT,
        HTTP,
    }
}
