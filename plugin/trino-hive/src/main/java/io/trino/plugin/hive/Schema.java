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

import io.airlift.slice.SizeOf;

import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record Schema(
        String serializationLibraryName,
        boolean isFullAcidTable,
        Map<String, String> serdeProperties)
{
    private static final int INSTANCE_SIZE = instanceSize(Schema.class);

    public Schema
    {
        requireNonNull(serializationLibraryName, "serializationLibraryName is null");
        requireNonNull(serdeProperties, "serdeProperties is null");
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(serializationLibraryName)
                + estimatedSizeOf(serdeProperties, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
    }
}
