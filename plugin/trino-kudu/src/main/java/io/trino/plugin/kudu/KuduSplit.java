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
package io.trino.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class KuduSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(KuduSplit.class);

    private final SchemaTableName schemaTableName;
    private final int primaryKeyColumnCount;
    private final byte[] serializedScanToken;
    private final int bucketNumber;

    @JsonCreator
    public KuduSplit(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("primaryKeyColumnCount") int primaryKeyColumnCount,
            @JsonProperty("serializedScanToken") byte[] serializedScanToken,
            @JsonProperty("bucketNumber") int bucketNumber)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.primaryKeyColumnCount = primaryKeyColumnCount;
        this.serializedScanToken = requireNonNull(serializedScanToken, "serializedScanToken is null");
        checkArgument(bucketNumber >= 0, "bucketNumber is negative");
        this.bucketNumber = bucketNumber;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public byte[] getSerializedScanToken()
    {
        return serializedScanToken;
    }

    @JsonProperty
    public int getPrimaryKeyColumnCount()
    {
        return primaryKeyColumnCount;
    }

    @JsonProperty
    public int getBucketNumber()
    {
        return bucketNumber;
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
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + schemaTableName.getRetainedSizeInBytes()
                + sizeOf(serializedScanToken);
    }
}
