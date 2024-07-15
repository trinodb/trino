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

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record HivePartitionKey(String name, String value)
{
    private static final int INSTANCE_SIZE = instanceSize(HivePartitionKey.class);

    public static final String HIVE_DEFAULT_DYNAMIC_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    public HivePartitionKey
    {
        requireNonNull(name, "name is null");
        requireNonNull(value, "value is null");
        value = value.equals(HIVE_DEFAULT_DYNAMIC_PARTITION) ? "\\N" : value;
    }

    public long estimatedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(name) + estimatedSizeOf(value);
    }
}
