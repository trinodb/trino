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

import io.trino.metastore.HiveType;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SourcePage;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucket;
import static java.lang.String.format;

public class BucketAdapter
{
    private final int[] bucketColumns;
    private final HiveBucketing.BucketingVersion bucketingVersion;
    private final int bucketToKeep;
    private final int tableBucketCount;
    private final int partitionBucketCount; // for sanity check only
    private final List<TypeInfo> typeInfoList;

    public BucketAdapter(HivePageSourceProvider.BucketAdaptation bucketAdaptation)
    {
        this.bucketColumns = bucketAdaptation.getBucketColumnIndices();
        this.bucketingVersion = bucketAdaptation.getBucketingVersion();
        this.bucketToKeep = bucketAdaptation.getBucketToKeep();
        this.typeInfoList = bucketAdaptation.getBucketColumnHiveTypes().stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());
        this.tableBucketCount = bucketAdaptation.getTableBucketCount();
        this.partitionBucketCount = bucketAdaptation.getPartitionBucketCount();
    }

    @Nullable
    public SourcePage filterPageToEligibleRowsOrDiscard(SourcePage page)
    {
        IntArrayList ids = new IntArrayList(page.getPositionCount());
        Page bucketColumnsPage = page.getColumns(bucketColumns);
        for (int position = 0; position < page.getPositionCount(); position++) {
            int bucket = getHiveBucket(bucketingVersion, tableBucketCount, typeInfoList, bucketColumnsPage, position);
            if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                throw new TrinoException(HIVE_INVALID_BUCKET_FILES, format(
                        "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                        bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
            }
            if (bucket == bucketToKeep) {
                ids.add(position);
            }
        }
        int retainedRowCount = ids.size();
        if (retainedRowCount == 0) {
            return null;
        }
        if (retainedRowCount == page.getPositionCount()) {
            return page;
        }
        page.selectPositions(ids.elements(), 0, retainedRowCount);
        return page;
    }
}
