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

import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveBucketFunction
        implements BucketFunction
{
    private final BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final List<TypeInfo> typeInfos;

    public HiveBucketFunction(BucketingVersion bucketingVersion, int bucketCount, List<HiveType> hiveTypes)
    {
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.typeInfos = hiveTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(Collectors.toList());
    }

    @Override
    public int getBucket(Page page, int position)
    {
        return HiveBucketing.getHiveBucket(bucketingVersion, bucketCount, typeInfos, page, position);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", bucketingVersion)
                .add("bucketCount", bucketCount)
                .add("typeInfos", typeInfos)
                .toString();
    }
}
