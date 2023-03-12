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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.util.ForwardingRecordCursor;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HivePageSource.BucketValidator.VALIDATION_STRIDE;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucket;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveBucketValidationRecordCursor
        extends ForwardingRecordCursor
{
    private final RecordCursor delegate;
    private final Path path;
    private final int[] bucketColumnIndices;
    private final List<Class<?>> javaTypeList;
    private final List<TypeInfo> typeInfoList;
    private final BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final int expectedBucket;

    private final Object[] scratch;

    private int validationCounter;

    public HiveBucketValidationRecordCursor(
            Path path,
            int[] bucketColumnIndices,
            List<HiveType> bucketColumnTypes,
            BucketingVersion bucketingVersion,
            int bucketCount,
            int expectedBucket,
            TypeManager typeManager,
            RecordCursor delegate)
    {
        this.path = requireNonNull(path, "path is null");
        this.bucketColumnIndices = requireNonNull(bucketColumnIndices, "bucketColumnIndices is null");
        requireNonNull(bucketColumnTypes, "bucketColumnTypes is null");
        this.javaTypeList = bucketColumnTypes.stream()
                .map(HiveType::getTypeSignature)
                .map(typeManager::getType)
                .map(Type::getJavaType)
                .collect(toImmutableList());
        this.typeInfoList = bucketColumnTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.expectedBucket = expectedBucket;
        this.delegate = requireNonNull(delegate, "delegate is null");

        this.scratch = new Object[bucketColumnTypes.size()];
    }

    @VisibleForTesting
    @Override
    public RecordCursor delegate()
    {
        return delegate;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!delegate.advanceNextPosition()) {
            return false;
        }

        if (validationCounter > 0) {
            validationCounter--;
            return true;
        }
        validationCounter = VALIDATION_STRIDE - 1;

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
                throw new VerifyException("Unknown Java type: " + javaType);
            }
        }

        int bucket = getHiveBucket(bucketingVersion, bucketCount, typeInfoList, scratch);
        if (bucket != expectedBucket) {
            throw new TrinoException(HIVE_INVALID_BUCKET_FILES,
                    format("Hive table is corrupt. File '%s' is for bucket %s, but contains a row for bucket %s.", path, expectedBucket, bucket));
        }

        return true;
    }
}
