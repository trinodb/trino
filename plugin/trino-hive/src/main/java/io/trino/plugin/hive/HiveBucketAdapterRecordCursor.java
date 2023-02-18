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

import io.airlift.slice.Slice;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.util.ForwardingRecordCursor;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveBucketAdapterRecordCursor
        extends ForwardingRecordCursor
{
    private final RecordCursor delegate;
    private final int[] bucketColumnIndices;
    private final List<Class<?>> javaTypeList;
    private final List<TypeInfo> typeInfoList;
    private final BucketingVersion bucketingVersion;
    private final int tableBucketCount;
    private final int partitionBucketCount;
    private final int bucketToKeep;

    private final Object[] scratch;

    public HiveBucketAdapterRecordCursor(
            int[] bucketColumnIndices,
            List<HiveType> bucketColumnHiveTypes,
            BucketingVersion bucketingVersion,
            int tableBucketCount,
            int partitionBucketCount,
            int bucketToKeep,
            TypeManager typeManager,
            RecordCursor delegate)
    {
        this.bucketColumnIndices = requireNonNull(bucketColumnIndices, "bucketColumnIndices is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(bucketColumnHiveTypes, "bucketColumnHiveTypes is null");
        this.javaTypeList = bucketColumnHiveTypes.stream()
                .map(HiveType::getTypeSignature)
                .map(typeManager::getType)
                .map(Type::getJavaType)
                .collect(toImmutableList());
        this.typeInfoList = bucketColumnHiveTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.tableBucketCount = tableBucketCount;
        this.partitionBucketCount = partitionBucketCount;
        this.bucketToKeep = bucketToKeep;

        this.scratch = new Object[bucketColumnHiveTypes.size()];
    }

    @Override
    protected RecordCursor delegate()
    {
        return delegate;
    }

    @Override
    public boolean advanceNextPosition()
    {
        while (true) {
            if (Thread.interrupted()) {
                // Stop processing if the query has been destroyed.
                Thread.currentThread().interrupt();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "RecordCursor was interrupted");
            }

            boolean hasNextPosition = delegate.advanceNextPosition();
            if (!hasNextPosition) {
                return false;
            }
            for (int i = 0; i < scratch.length; i++) {
                int index = bucketColumnIndices[i];
                if (delegate.isNull(index)) {
                    scratch[i] = null;
                    continue;
                }
                Class<?> javaType = javaTypeList.get(i);
                if (javaType == boolean.class) {
                    scratch[i] = delegate.getBoolean(index);
                }
                else if (javaType == long.class) {
                    scratch[i] = delegate.getLong(index);
                }
                else if (javaType == double.class) {
                    scratch[i] = delegate.getDouble(index);
                }
                else if (javaType == Slice.class) {
                    scratch[i] = delegate.getSlice(index);
                }
                else if (javaType == Block.class) {
                    scratch[i] = delegate.getObject(index);
                }
                else {
                    throw new UnsupportedOperationException("Unknown java type: " + javaType);
                }
            }
            int bucket = HiveBucketing.getHiveBucket(bucketingVersion, tableBucketCount, typeInfoList, scratch);
            if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                throw new TrinoException(HIVE_INVALID_BUCKET_FILES, format(
                        "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                        bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
            }
            if (bucket == bucketToKeep) {
                return true;
            }
        }
    }
}
