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
package io.trino.plugin.deltalake;

import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public record DeltaLakeMergeResult(
        List<String> partitionValues,
        Optional<String> oldFile,
        Optional<DeletionVectorEntry> oldDeletionVector,
        Optional<DataFileInfo> newFile)
{
    public DeltaLakeMergeResult
    {
        // Immutable list does not allow nulls
        // noinspection Java9CollectionFactory
        partitionValues = unmodifiableList(new ArrayList<>(requireNonNull(partitionValues, "partitionValues is null")));
        requireNonNull(oldFile, "oldFile is null");
        requireNonNull(oldDeletionVector, "oldDeletionVector is null");
        requireNonNull(newFile, "newFile is null");
        checkArgument(oldFile.isPresent() || newFile.isPresent(), "old or new must be present");
        checkArgument(oldDeletionVector.isEmpty() || oldFile.isPresent(), "oldDeletionVector is present only when oldFile is present");
    }
}
