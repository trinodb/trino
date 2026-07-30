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
package io.trino.plugin.dynamodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class DynamoDbSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DynamoDbSplit.class);

    private final String tableName;
    private final int segment;
    private final int totalSegments;

    @JsonCreator
    public DynamoDbSplit(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("segment") int segment,
            @JsonProperty("totalSegments") int totalSegments)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.segment = segment;
        this.totalSegments = totalSegments;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public int getSegment()
    {
        return segment;
    }

    @JsonProperty
    public int getTotalSegments()
    {
        return totalSegments;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(tableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("segment", segment)
                .add("totalSegments", totalSegments)
                .toString();
    }
}
