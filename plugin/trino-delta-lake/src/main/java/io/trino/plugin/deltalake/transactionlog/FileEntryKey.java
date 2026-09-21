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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

// A logical file is identified by its path and deletion vector, so 'add' and 'remove' entries which share a path
// but carry different deletion vectors refer to different files and must not cancel each other out.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation
public record FileEntryKey(String path, Optional<String> deletionVectorId)
{
    public FileEntryKey
    {
        requireNonNull(path, "path is null");
        requireNonNull(deletionVectorId, "deletionVectorId is null");
    }

    public static FileEntryKey of(AddFileEntry entry)
    {
        return new FileEntryKey(entry.getPath(), entry.getDeletionVector().map(DeletionVectorEntry::uniqueId));
    }

    public static FileEntryKey of(RemoveFileEntry entry)
    {
        return new FileEntryKey(entry.path(), entry.deletionVector().map(DeletionVectorEntry::uniqueId));
    }
}
