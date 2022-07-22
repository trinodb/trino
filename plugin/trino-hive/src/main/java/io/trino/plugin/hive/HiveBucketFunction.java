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
package io.trino.plugin.hive;

import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HiveBucketFunction
        implements BucketFunction
{
    private final BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final List<TypeInfo> typeInfos;
    private final List<Type> partitionColumnTypes;
    private final List<List<NullableValue>> partitions;

    private static List<List<NullableValue>> getPartitionValues(List<String> partitions, List<Type> partitionColumnTypes)
    {
        return partitions.stream().map(partitionName -> {
            List<String> values = HivePartitionManager.extractPartitionValues(partitionName);
            ImmutableList.Builder<NullableValue> builder = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                NullableValue parsedValue = HiveUtil.parsePartitionValue(partitionName, values.get(i), partitionColumnTypes.get(i));
                builder.add(parsedValue);
            }
            return builder.build();
        }).collect(toImmutableList());
    }

    public HiveBucketFunction(BucketingVersion bucketingVersion, int bucketCount, List<HiveType> hiveTypes, List<Type> partitionColumnTypes, List<String> partitions)
    {
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.typeInfos = requireNonNull(hiveTypes, "hiveTypes is null").stream()
                .map(HiveType::getTypeInfo)
                .collect(Collectors.toList());
        this.partitionColumnTypes = requireNonNull(partitionColumnTypes, "partitionColumnTypes is null");
        this.partitions = getPartitionValues(requireNonNull(partitions, "partitions is null"), partitionColumnTypes);
    }

    @Override
    public int getBucket(Page page, int position)
    {
        int bucket = 0;
        if (bucketCount > 1) {
            bucket = HiveBucketing.getHiveBucket(bucketingVersion, bucketCount, typeInfos, page, position);
        }
        if (partitions.isEmpty()) {
            return bucket;
        }

        int partition = 0;
        ImmutableList.Builder<NullableValue> partitionValueBuilder = ImmutableList.builder();
        for (int i = 0; i < partitionColumnTypes.size(); i++) {
            Block block = page.getBlock(i);
            Type type = partitionColumnTypes.get(i);
            NullableValue value;
            if (block.isNull(position)) {
                value = NullableValue.asNull(type);
            }
            else {
                value = NullableValue.of(type, type.getObject(block, position));
            }
            partitionValueBuilder.add(value);
        }
        // TODO: don't do a linear search!
        List<NullableValue> partitionValue = partitionValueBuilder.build();
        for (int i = 0; i < partitions.size(); i++) {
            if (partitions.get(i).equals(partitionValue)) {
                partition = i % bucketCount;
                break;
            }
        }

        return bucket + (partition * bucketCount);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("version", bucketingVersion)
                .add("bucketCount", bucketCount)
                .add("typeInfos", typeInfos);
        if (!partitions.isEmpty()) {
            helper = helper.add("partitions", partitions)
                    .add("partitionColumnTypes", partitionColumnTypes);
        }
        return helper.toString();
    }
}
