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
package io.trino.loki;

import io.trino.loki.model.LogEntry;
import io.trino.loki.model.Streams;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class LokiStreamsRecordCursor
        extends LokiRecordCursor
{

    private final Iterator<LabelledEntry> entryItr;

    record LabelledEntry(LogEntry entry, Map<String, String> labels) {}

    private LabelledEntry current;

    public LokiStreamsRecordCursor(List<LokiColumnHandle> columnHandles, Streams streamsResult)
    {
        super(columnHandles);

        this.entryItr = streamsResult.getStreams()
                .stream()
                .flatMap(stream -> stream.values().stream()
                        .map(value -> new LabelledEntry(value, stream.labels()))).iterator();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!entryItr.hasNext()) {
            return false;
        }
        current = entryItr.next();
        return true;
    }

    @Override
    Object getEntryValue(int field)
    {
        checkState(current != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return switch (columnIndex) {
            case 0 -> getSqlMapFromMap(columnHandles.get(columnIndex).type(), current.labels);
            case 1 -> current.entry.getTs();
            case 2 -> current.entry.getLine();
            default -> null;
        };
    }
}
