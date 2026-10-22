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
package org.apache.iceberg;

import org.apache.iceberg.util.JsonUtil;

public final class IcebergPartitionSpecUtils
{
    private IcebergPartitionSpecUtils() {}

    /**
     * Binds a partition spec to a schema which may lack some of its source columns, e.g. when reading an older
     * snapshot of a table that was later partitioned by a new column. {@link PartitionSpecParser#fromJson(Schema, String)}
     * rejects such specs; this keeps the fields of missing columns, as {@link TableMetadata} does when binding a table's specs.
     */
    public static PartitionSpec bindUnchecked(Schema schema, String partitionSpecJson)
    {
        return JsonUtil.parse(partitionSpecJson, PartitionSpecParser::fromJson).bindUnchecked(schema);
    }
}
