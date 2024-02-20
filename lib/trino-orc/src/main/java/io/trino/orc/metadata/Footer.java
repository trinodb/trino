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
package io.trino.orc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.orc.metadata.statistics.ColumnStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Objects.requireNonNull;

public class Footer
{
    private final long numberOfRows;
    private final OptionalInt rowsInRowGroup;
    private final List<StripeInformation> stripes;
    private final ColumnMetadata<OrcType> types;
    private final Optional<ColumnMetadata<ColumnStatistics>> fileStats;
    private final Map<String, Slice> userMetadata;
    private final Optional<Integer> writerId;

    public Footer(
            long numberOfRows,
            OptionalInt rowsInRowGroup,
            List<StripeInformation> stripes,
            ColumnMetadata<OrcType> types,
            Optional<ColumnMetadata<ColumnStatistics>> fileStats,
            Map<String, Slice> userMetadata,
            Optional<Integer> writerId)
    {
        this.numberOfRows = numberOfRows;
        rowsInRowGroup.ifPresent(value -> checkArgument(value > 0, "rowsInRowGroup must be at least 1"));
        this.rowsInRowGroup = rowsInRowGroup;
        this.stripes = ImmutableList.copyOf(requireNonNull(stripes, "stripes is null"));
        this.types = requireNonNull(types, "types is null");
        this.fileStats = requireNonNull(fileStats, "fileStats is null");
        requireNonNull(userMetadata, "userMetadata is null");
        this.userMetadata = ImmutableMap.copyOf(transformValues(userMetadata, Slice::copy));
        this.writerId = requireNonNull(writerId, "writerId is null");
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public OptionalInt getRowsInRowGroup()
    {
        return rowsInRowGroup;
    }

    public List<StripeInformation> getStripes()
    {
        return stripes;
    }

    public ColumnMetadata<OrcType> getTypes()
    {
        return types;
    }

    public Optional<ColumnMetadata<ColumnStatistics>> getFileStats()
    {
        return fileStats;
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(transformValues(userMetadata, Slice::copy));
    }

    public Optional<Integer> getWriterId()
    {
        return writerId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("rowsInRowGroup", rowsInRowGroup)
                .add("stripes", stripes)
                .add("types", types)
                .add("columnStatistics", fileStats)
                .add("userMetadata", userMetadata.keySet())
                .add("writerId", writerId)
                .toString();
    }
}
