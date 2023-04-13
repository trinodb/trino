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
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.addFileEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.metadataEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.protocolEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.removeFileEntry;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.transactionEntry;
import static org.testng.Assert.assertEquals;

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
        ProtocolEntry protocol2 = new ProtocolEntry(3, 4, Optional.empty(), Optional.empty());
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

        AddFileEntry addA1 = new AddFileEntry("a", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of());
        RemoveFileEntry removeA1 = new RemoveFileEntry("a", 1, true);
        AddFileEntry addA2 = new AddFileEntry("a", Map.of(), 2, 1, true, Optional.empty(), Optional.empty(), Map.of());
        AddFileEntry addB = new AddFileEntry("b", Map.of(), 1, 1, true, Optional.empty(), Optional.empty(), Map.of());
        RemoveFileEntry removeB = new RemoveFileEntry("b", 1, true);
        RemoveFileEntry removeC = new RemoveFileEntry("c", 1, true);
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
        assertEquals(expectedCheckpoint, builder.build());
    }
}
