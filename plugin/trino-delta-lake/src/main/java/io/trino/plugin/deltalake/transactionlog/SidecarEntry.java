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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
public record SidecarEntry(String path, long sizeInBytes, long modificationTime, Optional<Map<String, String>> tags)
{
    public SidecarEntry
    {
        checkArgument(sizeInBytes > 0, "sizeInBytes is not positive: %s", sizeInBytes);
        requireNonNull(path, "path is null");
        requireNonNull(tags, "tags is null");
        tags = tags.map(ImmutableMap::copyOf);
    }
}
