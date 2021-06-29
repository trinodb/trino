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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class KuduBucketFunction
        implements BucketFunction
{
    private final List<Integer> bucketChannels;
    private final List<Type> bucketChannelTypes;

    private final List<HashBucketSchema> hashBucketSchemas;
    private final Schema schema;

    KuduBucketFunction(List<Integer> bucketChannels, List<Type> bucketChannelTypes, KuduTable kuduTable)
    {
        this.bucketChannels = ImmutableList.copyOf(requireNonNull(bucketChannels));
        this.bucketChannelTypes = ImmutableList.copyOf(requireNonNull(bucketChannelTypes));
        checkArgument(bucketChannels.size() == bucketChannelTypes.size());

        this.hashBucketSchemas = kuduTable.getPartitionSchema().getHashBucketSchemas();
        this.schema = kuduTable.getSchema();
    }

    @Override
    public int getBucket(Page page, int position)
    {
        checkArgument(this.bucketChannelTypes.size() == page.getChannelCount());
        PartialRow partialRow = new PartialRow(this.schema);
        List<Integer> bucketIds = this.hashBucketSchemas.stream()
                .map(hashBucketSchema -> this.calculateSchemaLevelBucketId(page, partialRow, hashBucketSchema, position))
                .collect(toImmutableList());

        return getBucket(bucketIds, hashBucketSchemas);
    }

    static int getBucket(List<Integer> bucketIds, List<HashBucketSchema> hashBucketSchemas)
    {
        int bucketId = 0;
        for (int i = 0; i < bucketIds.size(); i++) {
            int dimensionBucket = 1;
            for (int j = 0; j < i; j++) {
                dimensionBucket *= hashBucketSchemas.get(j).getNumBuckets();
            }
            dimensionBucket *= bucketIds.get(i);
            bucketId += dimensionBucket;
        }
        return bucketId;
    }

    private int calculateSchemaLevelBucketId(Page page, PartialRow partialRow, HashBucketSchema hashBucketSchema, int position)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            Type type = this.bucketChannelTypes.get(channel);
            Integer bucketChannel = this.bucketChannels.get(channel);
            if (BOOLEAN.equals(type)) {
                partialRow.addBoolean(bucketChannel, BOOLEAN.getBoolean(block, position));
            }
            else if (BIGINT.equals(type)) {
                partialRow.addLong(bucketChannel, type.getLong(block, position));
            }
            else if (INTEGER.equals(type)) {
                partialRow.addInt(bucketChannel, toIntExact(type.getLong(block, position)));
            }
            else if (SMALLINT.equals(type)) {
                partialRow.addShort(bucketChannel, Shorts.checkedCast(type.getLong(block, position)));
            }
            else if (TINYINT.equals(type)) {
                partialRow.addByte(bucketChannel, SignedBytes.checkedCast(type.getLong(block, position)));
            }
            else if (DOUBLE.equals(type)) {
                partialRow.addDouble(bucketChannel, type.getDouble(block, position));
            }
            else if (VARCHAR.equals(type)) {
                partialRow.addString(bucketChannel, type.getSlice(block, position).toStringUtf8());
            }
            else {
                throw new UnsupportedOperationException("Grouped execution is not supported for type: " + type);
            }
        }
        return KeyEncoderAccessor.getHashBucket(partialRow, hashBucketSchema);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketChannels", bucketChannels)
                .toString();
    }
}
