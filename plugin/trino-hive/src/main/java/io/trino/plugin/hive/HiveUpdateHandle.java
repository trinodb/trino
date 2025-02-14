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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;

import java.util.List;

public class HiveUpdateHandle
        extends HivePartitioningHandle
{
    @JsonCreator
    public HiveUpdateHandle(
            @JsonProperty("bucketingVersion") BucketingVersion bucketingVersion,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("hiveBucketTypes") List<HiveType> hiveTypes,
            @JsonProperty("usePartitionedBucketing") boolean usePartitionedBucketing)
    {
        super(bucketingVersion, bucketCount, hiveTypes, usePartitionedBucketing);
    }
}
