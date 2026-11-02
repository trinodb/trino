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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.addFileEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.metadataEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.protocolEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.removeFileEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.transactionEntry;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCheckpointBuilder
{
    @Test
    public void testCheckpointBuilder()
    {
        CheckpointBuilder builder = new CheckpointBuilder();

        MetadataEntry metadata1 = new MetadataEntry("1", "", "", new MetadataEntry.Format("", Map.of()), "", List.of(), Map.of(), 1);
        MetadataEntry metadata2 = new MetadataEntry("2", "", "", new MetadataEntry.Format("", Map.of()), "", List.of(), Map.of(), 1);
        builder.addLogEntry(metadataEntry(metadata1));
        builder.addLogEntry(metadataEntry(metadata2));

        ProtocolEntry protocol1 = new ProtocolEntry(1, 2, Optional.empty(), Optional.empty());
        ProtocolEntry protocol2 = new ProtocolEntry(2, 4, Optional.empty(), Optional.empty());
        builder.addLogEntry(protocolEntry(protocol1));
        builder.addLogEntry(protocolEntry(protocol2));

        TransactionEntry app1TransactionV1 = new TransactionEntry("app1", 1, 1);
        TransactionEntry app1TransactionV2 = new TransactionEntry("app1", 2, 2);
        TransactionEntry app1TransactionV3 = new TransactionEntry("app1", 3, 3);
        TransactionEntry app2TransactionV5 = new TransactionEntry("app2", 5, 5);
        builder.addLogEntry(transactionEntry(app1TransactionV2));
        builder.addLogEntry(transactionEntry(app1TransactionV3));
        builder.addLogEntry(transactionEntry(app1TransactionV1));
        builder.addLogEntry(transactionEntry(app2TransactionV5));

        AddFileEntry addA1 = new AddFileEntry("a", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of(), Optional.empty());
        RemoveFileEntry removeA1 = new RemoveFileEntry("a", Map.of(), 1, true, Optional.empty());
        AddFileEntry addA2 = new AddFileEntry("a", Map.of(), 2, 1, true, Optional.empty(), Optional.empty(), Map.of(), Optional.empty());
        AddFileEntry addB = new AddFileEntry("b", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of(), Optional.empty());
        RemoveFileEntry removeB = new RemoveFileEntry("b", Map.of(), 1, true, Optional.empty());
        RemoveFileEntry removeC = new RemoveFileEntry("c", Map.of(), 1, true, Optional.empty());
        builder.addLogEntry(addFileEntry(addA1));
        builder.addLogEntry(removeFileEntry(removeA1));
        builder.addLogEntry(addFileEntry(addA2));
        builder.addLogEntry(addFileEntry(addB));
        builder.addLogEntry(removeFileEntry(removeB));
        builder.addLogEntry(removeFileEntry(removeC));

        CheckpointEntries expectedCheckpoint = new CheckpointEntries(
                metadata2,
                protocol2,
                Set.of(app1TransactionV3, app2TransactionV5),
                Set.of(addA2),
                Set.of(removeB, removeC));
        assertThat(expectedCheckpoint).isEqualTo(builder.build());
    }

    @Test
    public void testCheckpointBuilderWithDeletionVector()
    {
        MetadataEntry metadata = new MetadataEntry("1", "", "", new MetadataEntry.Format("", Map.of()), "", List.of(), Map.of(), 1);
        ProtocolEntry protocol = new ProtocolEntry(3, 7, Optional.empty(), Optional.empty());

        DeletionVectorEntry deletionVector = new DeletionVectorEntry("u", "hR{.8.y4^dHj2[P[Sd0J", OptionalInt.of(1), 34, 1);
        AddFileEntry addWithDeletionVector = new AddFileEntry("a", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of(), Optional.of(deletionVector));
        // The 'remove' entry tombstones the version of the file which has no deletion vector, a different logical file than the 'add' entry above
        RemoveFileEntry remove = new RemoveFileEntry("a", Map.of(), 1, true, Optional.empty());

        CheckpointEntries expectedCheckpoint = new CheckpointEntries(metadata, protocol, Set.of(), Set.of(addWithDeletionVector), Set.of(remove));

        // Spark writes the 'add' before the 'remove'
        CheckpointBuilder addBeforeRemove = new CheckpointBuilder();
        addBeforeRemove.addLogEntry(metadataEntry(metadata));
        addBeforeRemove.addLogEntry(protocolEntry(protocol));
        addBeforeRemove.addLogEntry(addFileEntry(addWithDeletionVector));
        addBeforeRemove.addLogEntry(removeFileEntry(remove));
        assertThat(addBeforeRemove.build()).isEqualTo(expectedCheckpoint);

        // Trino writes the 'remove' before the 'add'
        CheckpointBuilder removeBeforeAdd = new CheckpointBuilder();
        removeBeforeAdd.addLogEntry(metadataEntry(metadata));
        removeBeforeAdd.addLogEntry(protocolEntry(protocol));
        removeBeforeAdd.addLogEntry(removeFileEntry(remove));
        removeBeforeAdd.addLogEntry(addFileEntry(addWithDeletionVector));
        assertThat(removeBeforeAdd.build()).isEqualTo(expectedCheckpoint);
    }
}
