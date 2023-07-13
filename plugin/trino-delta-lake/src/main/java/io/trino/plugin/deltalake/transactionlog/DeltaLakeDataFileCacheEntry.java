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

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

@Immutable
public final class DeltaLakeDataFileCacheEntry
{
    private static final int INSTANCE_SIZE = instanceSize(DeltaLakeDataFileCacheEntry.class);

    private final long version;
    private final List<AddFileEntry> activeFiles;
    private volatile long retainedSize = -1;

    public DeltaLakeDataFileCacheEntry(long version, List<AddFileEntry> activeFiles)
    {
        this.version = version;
        this.activeFiles = ImmutableList.copyOf(requireNonNull(activeFiles, "activeFiles is null"));
    }

    public DeltaLakeDataFileCacheEntry withUpdatesApplied(List<DeltaLakeTransactionLogEntry> newEntries, long newVersion)
    {
        Map<String, AddFileEntry> activeJsonEntries = new LinkedHashMap<>();
        HashSet<String> removedFiles = new HashSet<>();

        // The json entries containing the last few entries in the log need to be applied on top of the parquet snapshot:
        // - Any files which have been removed need to be excluded
        // - Any files with newer add actions need to be updated with the most recent metadata
        newEntries.forEach(deltaLakeTransactionLogEntry -> {
            AddFileEntry addEntry = deltaLakeTransactionLogEntry.getAdd();
            if (addEntry != null) {
                activeJsonEntries.put(addEntry.getPath(), addEntry);
            }

            RemoveFileEntry removeEntry = deltaLakeTransactionLogEntry.getRemove();
            if (removeEntry != null) {
                activeJsonEntries.remove(removeEntry.getPath());
                removedFiles.add(removeEntry.getPath());
            }
        });

        Stream<AddFileEntry> filteredExistingEntries = activeFiles.stream()
                .filter(addEntry -> !removedFiles.contains(addEntry.getPath()) && !activeJsonEntries.containsKey(addEntry.getPath()));
        List<AddFileEntry> updatedActiveFileList =
                Stream.concat(filteredExistingEntries, activeJsonEntries.values().stream())
                        .collect(toImmutableList());

        return new DeltaLakeDataFileCacheEntry(newVersion, updatedActiveFileList);
    }

    public long getVersion()
    {
        return version;
    }

    public List<AddFileEntry> getActiveFiles()
    {
        return activeFiles;
    }

    public long getRetainedSizeInBytes()
    {
        if (retainedSize == -1) {
            retainedSize = INSTANCE_SIZE + activeFiles.stream()
                    .mapToLong(AddFileEntry::getRetainedSizeInBytes)
                    .sum();
        }
        return retainedSize;
    }
}
