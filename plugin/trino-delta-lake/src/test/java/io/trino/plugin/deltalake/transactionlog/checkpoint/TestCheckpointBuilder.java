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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.readLines;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.addFileEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.metadataEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.protocolEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.removeFileEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.transactionEntry;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.parseJson;
import static java.nio.charset.StandardCharsets.UTF_8;
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
    public void testDeletionVectorAddsBeforeRemoves()
    {
        assertDeletionVectorUpdates(true);
    }

    @Test
    public void testDeletionVectorRemovesBeforeAdds()
    {
        assertDeletionVectorUpdates(false);
    }

    private static void assertDeletionVectorUpdates(boolean addBeforeRemove)
    {
        CheckpointBuilder builder = new CheckpointBuilder();
        builder.addLogEntry(metadataEntry(new MetadataEntry("1", "", "", new MetadataEntry.Format("parquet", Map.of()), "", List.of(), Map.of(), 1)));
        builder.addLogEntry(protocolEntry(new ProtocolEntry(3, 7, Optional.of(Set.of("deletionVectors")), Optional.of(Set.of("deletionVectors")))));

        Optional<DeletionVectorEntry> previousDeletionVector = Optional.empty();
        builder.addLogEntry(addFileEntry(new AddFileEntry("a", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of(), previousDeletionVector)));
        List<RemoveFileEntry> tombstones = new ArrayList<>();
        for (DeletionVectorEntry deletionVector : List.of(
                new DeletionVectorEntry("p", "file:///deletion_vector_1.bin", OptionalInt.of(1), 34, 1),
                new DeletionVectorEntry("p", "file:///deletion_vector_1.bin", OptionalInt.of(39), 36, 2),
                new DeletionVectorEntry("p", "file:///deletion_vector_2.bin", OptionalInt.of(1), 38, 3))) {
            AddFileEntry add = new AddFileEntry("a", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of(), Optional.of(deletionVector));
            RemoveFileEntry remove = new RemoveFileEntry("a", Map.of(), 1, true, previousDeletionVector);
            if (addBeforeRemove) {
                builder.addLogEntry(addFileEntry(add));
                builder.addLogEntry(removeFileEntry(remove));
            }
            else {
                builder.addLogEntry(removeFileEntry(remove));
                builder.addLogEntry(addFileEntry(add));
            }
            tombstones.add(remove);
            CheckpointEntries checkpoint = builder.build();
            assertThat(checkpoint.addFileEntries()).containsExactly(add);
            assertThat(checkpoint.removeFileEntries()).containsExactlyInAnyOrderElementsOf(tombstones);
            previousDeletionVector = Optional.of(deletionVector);
        }

        RemoveFileEntry remove = new RemoveFileEntry("a", Map.of(), 2, true, previousDeletionVector);
        builder.addLogEntry(removeFileEntry(remove));
        tombstones.add(remove);
        assertThat(builder.build().addFileEntries()).isEmpty();
        assertThat(builder.build().removeFileEntries()).containsExactlyInAnyOrderElementsOf(tombstones);

        AddFileEntry restored = new AddFileEntry("a", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of(), previousDeletionVector);
        builder.addLogEntry(addFileEntry(restored));
        tombstones.remove(remove);
        assertThat(builder.build().addFileEntries()).containsExactly(restored);
        assertThat(builder.build().removeFileEntries()).containsExactlyInAnyOrderElementsOf(tombstones);
    }

    @Test
    public void testCheckpointFromDeletionVectorTransaction()
            throws IOException
    {
        CheckpointBuilder builder = new CheckpointBuilder();
        for (int version = 0; version <= 2; version++) {
            for (String line : readLines(getResource("deltalake/deletion_vector_pages/_delta_log/%020d.json".formatted(version)), UTF_8)) {
                builder.addLogEntry(parseJson(line));
            }
        }

        CheckpointEntries checkpoint = builder.build();
        assertThat(checkpoint.addFileEntries())
                .extracting(AddFileEntry::getPath)
                .containsExactlyInAnyOrder(
                        "part-00000-8e78760d-6337-4ce4-a7b8-3ce3e7c5be44-c000.snappy.parquet",
                        "part-00000-2007aafe-e829-46a6-b6cf-97f6ec686718.c000.snappy.parquet");
        assertThat(checkpoint.removeFileEntries())
                .extracting(RemoveFileEntry::path)
                .containsExactly("part-00000-8e78760d-6337-4ce4-a7b8-3ce3e7c5be44-c000.snappy.parquet");
    }
}
