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
package io.trino.plugin.loki;

import io.github.jeschkies.loki.client.model.LogEntry;
import io.github.jeschkies.loki.client.model.Streams;
import io.trino.spi.type.TimeZoneKey;

import java.util.Iterator;
import java.util.Map;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;

public class LokiStreamEntriesIterator
        implements LokiQueryResultIterator
{
    private final Iterator<LabelledEntry> entries;

    record LabelledEntry(LogEntry entry, Map<String, String> labels) {}

    private LabelledEntry current;

    public LokiStreamEntriesIterator(Streams streamsResult)
    {
        this.entries = streamsResult.getStreams()
                .stream()
                .flatMap(stream -> stream.values().stream()
                        .map(value -> new LabelledEntry(value, stream.labels()))).iterator();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!entries.hasNext()) {
            return false;
        }
        current = entries.next();
        return true;
    }

    @Override
    public Map<String, String> getLabels()
    {
        return current.labels;
    }

    @Override
    public long getTimestamp()
    {
        return toTimeWithTimeZone(current.entry.getTs());
    }

    @Override
    public Object getValue()
    {
        return current.entry.getLine();
    }

    /**
     * Stream entries have a timestamp in nanos. It's converted to a millisecond precision because we use a ShortTimeWithTimeZoneType.
     *
     * @param nanos nanoseconds since epoch.
     * @return time in Trino's packed format.
     */
    private static long toTimeWithTimeZone(Long nanos)
    {
        return packDateTimeWithZone(nanos / NANOSECONDS_PER_MILLISECOND, TimeZoneKey.UTC_KEY);
    }
}
