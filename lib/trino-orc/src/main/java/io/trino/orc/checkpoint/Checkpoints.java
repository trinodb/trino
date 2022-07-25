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
package io.trino.orc.checkpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.orc.StreamId;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static io.trino.orc.metadata.Stream.StreamKind.LENGTH;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.metadata.Stream.StreamKind.SECONDARY;
import static java.util.Objects.requireNonNull;

public final class Checkpoints
{
    private Checkpoints() {}

    public static Map<StreamId, StreamCheckpoint> getStreamCheckpoints(
            Set<OrcColumnId> columns,
            ColumnMetadata<OrcType> columnTypes,
            boolean compressed,
            int rowGroupId,
            ColumnMetadata<ColumnEncoding> columnEncodings,
            Map<StreamId, Stream> streams,
            Map<StreamId, List<RowGroupIndex>> columnIndexes)
            throws InvalidCheckpointException
    {
        ImmutableSetMultimap.Builder<OrcColumnId, StreamKind> streamKindsBuilder = ImmutableSetMultimap.builder();
        for (Stream stream : streams.values()) {
            streamKindsBuilder.put(stream.getColumnId(), stream.getStreamKind());
        }
        SetMultimap<OrcColumnId, StreamKind> streamKinds = streamKindsBuilder.build();

        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();
        for (Map.Entry<StreamId, List<RowGroupIndex>> entry : columnIndexes.entrySet()) {
            OrcColumnId columnId = entry.getKey().getColumnId();

            if (!columns.contains(columnId)) {
                continue;
            }

            List<Integer> positionsList = entry.getValue().get(rowGroupId).getPositions();

            ColumnEncodingKind columnEncoding = columnEncodings.get(columnId).getColumnEncodingKind();
            OrcTypeKind columnType = columnTypes.get(columnId).getOrcTypeKind();
            Set<StreamKind> availableStreams = streamKinds.get(columnId);

            ColumnPositionsList columnPositionsList = new ColumnPositionsList(columnId, columnType, positionsList);
            switch (columnType) {
                case BOOLEAN:
                    checkpoints.putAll(getBooleanColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList));
                    break;
                case BYTE:
                    checkpoints.putAll(getByteColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList));
                    break;
                case SHORT:
                case INT:
                case LONG:
                case DATE:
                    checkpoints.putAll(getLongColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case FLOAT:
                    checkpoints.putAll(getFloatColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList));
                    break;
                case DOUBLE:
                    checkpoints.putAll(getDoubleColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList));
                    break;
                case TIMESTAMP:
                case TIMESTAMP_INSTANT:
                    checkpoints.putAll(getTimestampColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case BINARY:
                case STRING:
                case VARCHAR:
                case CHAR:
                    checkpoints.putAll(getSliceColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case LIST:
                case MAP:
                    checkpoints.putAll(getListOrMapColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case STRUCT:
                    checkpoints.putAll(getStructColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList));
                    break;
                case DECIMAL:
                    checkpoints.putAll(getDecimalColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type " + columnType);
            }
        }
        return checkpoints.buildOrThrow();
    }

    public static StreamCheckpoint getDictionaryStreamCheckpoint(StreamId streamId, OrcTypeKind columnType, ColumnEncodingKind columnEncoding)
    {
        if (streamId.getStreamKind() == DICTIONARY_DATA) {
            switch (columnType) {
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStreamCheckpoint(createInputStreamCheckpoint(0, 0));
                default:
                    break;
            }
        }

        // dictionary length and data streams are unsigned long streams
        if (streamId.getStreamKind() == LENGTH || streamId.getStreamKind() == DATA) {
            if (columnEncoding == DICTIONARY_V2) {
                return new LongStreamV2Checkpoint(0, createInputStreamCheckpoint(0, 0));
            }
            else if (columnEncoding == DICTIONARY) {
                return new LongStreamV1Checkpoint(0, createInputStreamCheckpoint(0, 0));
            }
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType + " for dictionary stream " + streamId);
    }

    private static Map<StreamId, StreamCheckpoint> getBooleanColumnCheckpoints(
            OrcColumnId columnId,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getByteColumnCheckpoints(
            OrcColumnId columnId,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), new ByteStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getLongColumnCheckpoints(
            OrcColumnId columnId,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getFloatColumnCheckpoints(
            OrcColumnId columnId,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), new FloatStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getDoubleColumnCheckpoints(
            OrcColumnId columnId,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), new DoubleStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getTimestampColumnCheckpoints(
            OrcColumnId columnId,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        if (availableStreams.contains(SECONDARY)) {
            checkpoints.put(new StreamId(columnId, SECONDARY), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getSliceColumnCheckpoints(
            OrcColumnId columnId,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (encoding == DIRECT || encoding == DIRECT_V2) {
            if (availableStreams.contains(DATA)) {
                checkpoints.put(new StreamId(columnId, DATA), new ByteArrayStreamCheckpoint(compressed, positionsList));
            }

            if (availableStreams.contains(LENGTH)) {
                checkpoints.put(new StreamId(columnId, LENGTH), createLongStreamCheckpoint(encoding, compressed, positionsList));
            }
        }
        else if (encoding == DICTIONARY || encoding == DICTIONARY_V2) {
            if (availableStreams.contains(DATA)) {
                checkpoints.put(new StreamId(columnId, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding for slice column: " + encoding);
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getListOrMapColumnCheckpoints(
            OrcColumnId columnId,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(LENGTH)) {
            checkpoints.put(new StreamId(columnId, LENGTH), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getStructColumnCheckpoints(
            OrcColumnId columnId,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static Map<StreamId, StreamCheckpoint> getDecimalColumnCheckpoints(
            OrcColumnId columnId,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(columnId, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(columnId, DATA), new DecimalStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(SECONDARY)) {
            checkpoints.put(new StreamId(columnId, SECONDARY), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.buildOrThrow();
    }

    private static StreamCheckpoint createLongStreamCheckpoint(ColumnEncodingKind encoding, boolean compressed, ColumnPositionsList positionsList)
    {
        if (encoding == DIRECT_V2 || encoding == DICTIONARY_V2) {
            return new LongStreamV2Checkpoint(compressed, positionsList);
        }

        if (encoding == DIRECT || encoding == DICTIONARY) {
            return new LongStreamV1Checkpoint(compressed, positionsList);
        }

        throw new IllegalArgumentException("Unsupported encoding for long stream: " + encoding);
    }

    public static class ColumnPositionsList
    {
        private final OrcColumnId columnId;
        private final OrcTypeKind columnType;
        private final List<Integer> positionsList;
        private int index;

        private ColumnPositionsList(OrcColumnId columnId, OrcTypeKind columnType, List<Integer> positionsList)
        {
            this.columnId = requireNonNull(columnId, "columnId is null");
            this.columnType = requireNonNull(columnType, "columnType is null");
            this.positionsList = ImmutableList.copyOf(requireNonNull(positionsList, "positionsList is null"));
        }

        public int getIndex()
        {
            return index;
        }

        public boolean hasNextPosition()
        {
            return index < positionsList.size();
        }

        public int nextPosition()
        {
            if (!hasNextPosition()) {
                throw new InvalidCheckpointException("Not enough positions for column %s:%s checkpoints", columnId, columnType);
            }

            return positionsList.get(index++);
        }
    }
}
