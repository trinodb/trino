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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public record MemoryInsertTableHandle(long table, InsertMode mode, Set<Long> activeTableIds)
        implements ConnectorInsertTableHandle
{
    public enum InsertMode
    {
        APPEND, OVERWRITE
    }

    public MemoryInsertTableHandle
    {
        requireNonNull(mode, "mode is null");
        activeTableIds = ImmutableSet.copyOf(requireNonNull(activeTableIds, "activeTableIds is null"));
    }
}
