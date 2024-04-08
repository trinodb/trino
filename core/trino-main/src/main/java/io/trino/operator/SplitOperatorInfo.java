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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class SplitOperatorInfo
        implements OperatorInfo
{
    private static final long INSTANCE_SIZE = instanceSize(SplitOperatorInfo.class);

    private final CatalogHandle catalogHandle;
    // NOTE: this deserializes to a map instead of the expected type
    private final Object splitInfo;

    @JsonCreator
    public SplitOperatorInfo(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("splitInfo") Object splitInfo)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.splitInfo = splitInfo;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @JsonProperty
    public Object getSplitInfo()
    {
        return splitInfo;
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + splitInfoSize(splitInfo);
    }

    // TODO drop when splitInfo is non-opaque
    private long splitInfoSize(Object splitInfo)
    {
        return switch (splitInfo) {
            case Map map -> {
                try {
                    yield estimatedSizeOf(map,
                            key -> estimatedSizeOf((String) key),
                            value -> estimatedSizeOf((String) value));
                }
                catch (ClassCastException e) {
                    yield 0;
                }
            }
            case ConnectorSplit split -> split.getRetainedSizeInBytes();
            default -> 0;
        };
    }
}
