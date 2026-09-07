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
package io.trino.plugin.iceberg.delete;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface DeletionVectorWriter
{
    DeletionVectorWriter UNSUPPORTED_DELETION_VECTOR_WRITER = new DeletionVectorWriter()
    {
        @Override
        public void writeDeletionVectors(ConnectorSession session, Table icebergTable, IcebergTableHandle table, List<DeletionVectorInfo> deletionVectorInfos, RowDelta rowDelta)
        {
            throw new UnsupportedOperationException("Deletion Vectors are not supported");
        }

        @Override
        public void mergePreExistingDeletes(ConnectorSession session, Table icebergTable, long snapshotId, Map<String, DeletionVector.Builder> deletionVectorBuilders)
        {
            // no-op: pre-existing delete merging not supported when DVs are unsupported
        }

        @Override
        public void mergePreExistingDeletes(ConnectorSession session, TrinoFileSystem fileSystem, FileIO fileIo, Map<Integer, PartitionSpec> specsById, Snapshot snapshot, Map<String, DeletionVector.Builder> deletionVectorBuilders)
        {
            // no-op: pre-existing delete merging not supported when DVs are unsupported
        }
    };

    void writeDeletionVectors(
            ConnectorSession session,
            Table icebergTable,
            IcebergTableHandle table,
            List<DeletionVectorInfo> deletionVectorInfos,
            RowDelta rowDelta);

    void mergePreExistingDeletes(
            ConnectorSession session,
            Table icebergTable,
            long snapshotId,
            Map<String, DeletionVector.Builder> deletionVectorBuilders);

    void mergePreExistingDeletes(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            FileIO fileIo,
            Map<Integer, PartitionSpec> specsById,
            Snapshot snapshot,
            Map<String, DeletionVector.Builder> deletionVectorBuilders);

    record DeletionVectorInfo(String dataFilePath, Slice serializedDeletionVector, PartitionSpec partitionSpec, Optional<PartitionData> partitionData)
    {
        public DeletionVectorInfo
        {
            requireNonNull(dataFilePath, "dataFilePath is null");
            requireNonNull(serializedDeletionVector, "serializedDeletionVector is null");
            requireNonNull(partitionSpec, "partitionSpec is null");
            requireNonNull(partitionData, "partitionData is null");
        }
    }
}
