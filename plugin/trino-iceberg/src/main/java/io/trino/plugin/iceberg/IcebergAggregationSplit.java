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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * Split for a table scan whose aggregation results were computed from table metadata at planning time.
 * It produces exactly one empty row, over which the engine projects the precomputed aggregation values.
 */
public class IcebergAggregationSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergAggregationSplit.class);

    @JsonCreator
    public IcebergAggregationSplit() {}

    // Jackson cannot serialize a bean without properties
    @JsonProperty("aggregated")
    public boolean isAggregated()
    {
        return true;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
