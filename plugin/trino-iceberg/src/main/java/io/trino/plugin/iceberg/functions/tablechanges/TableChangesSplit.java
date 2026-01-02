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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.iceberg.RowLevelOperationMode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public record TableChangesSplit(
        RowLevelOperationMode operationMode,
        String partitionDataJson,
        String partitionSpecJson,
        List<ConnectorSplit> splits)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(TableChangesSplit.class);

    public TableChangesSplit
    {
        requireNonNull(operationMode, "operationMode is null");
        requireNonNull(partitionDataJson, "partitionDataJson is null");
        requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        splits = ImmutableList.copyOf(splits);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(partitionDataJson) +
                estimatedSizeOf(partitionSpecJson) +
                splits.stream().map(ConnectorSplit::getRetainedSizeInBytes).mapToLong(Long::longValue).sum();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("operationMode", operationMode)
                .add("partitionDataJson", partitionDataJson)
                .add("partitionSpecJson", partitionSpecJson)
                .add("splits", splits)
                .toString();
    }
}
