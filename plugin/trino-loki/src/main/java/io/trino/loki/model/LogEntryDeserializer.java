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
package io.trino.loki.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

class LogEntryDeserializer
        extends StdDeserializer<LogEntry>
{
    LogEntryDeserializer()
    {
        super(LogEntry.class);
    }

    @Override
    public LogEntry deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException
    {
        final JsonNode node = p.getCodec().readTree(p);
        LogEntry entry = new LogEntry();
        entry.setTs(node.get(0).asLong());
        entry.setLine(node.get(1).asText());
        return entry;
    }
}
