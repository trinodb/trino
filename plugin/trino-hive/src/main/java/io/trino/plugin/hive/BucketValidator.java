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

import io.trino.filesystem.Location;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucket;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BucketValidator
{
    // validate every ~100 rows but using a prime number
    public static final int VALIDATION_STRIDE = 97;

    private final Location path;
    private final int[] bucketColumnIndices;
    private final List<TypeInfo> bucketColumnTypes;
    private final HiveBucketing.BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final int expectedBucket;

    public BucketValidator(
            Location path,
            int[] bucketColumnIndices,
            List<TypeInfo> bucketColumnTypes,
            HiveBucketing.BucketingVersion bucketingVersion,
            int bucketCount,
            int expectedBucket)
    {
        this.path = requireNonNull(path, "path is null");
        this.bucketColumnIndices = requireNonNull(bucketColumnIndices, "bucketColumnIndices is null");
        this.bucketColumnTypes = requireNonNull(bucketColumnTypes, "bucketColumnTypes is null");
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.expectedBucket = expectedBucket;
        checkArgument(bucketColumnIndices.length == bucketColumnTypes.size(), "indices and types counts mismatch");
    }

    public void validate(Page page)
    {
        Page bucketColumnsPage = page.getColumns(bucketColumnIndices);
        for (int position = 0; position < page.getPositionCount(); position += VALIDATION_STRIDE) {
            int bucket = getHiveBucket(bucketingVersion, bucketCount, bucketColumnTypes, bucketColumnsPage, position);
            if (bucket != expectedBucket) {
                throw new TrinoException(
                        HIVE_INVALID_BUCKET_FILES,
                        format("Hive table is corrupt. File '%s' is for bucket %s, but contains a row for bucket %s.", path, expectedBucket, bucket));
            }
        }
    }
}
