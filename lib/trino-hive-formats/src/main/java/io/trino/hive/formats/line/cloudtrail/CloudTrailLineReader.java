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
package io.trino.hive.formats.line.cloudtrail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/*
    Cloudtrail reader should be used together with a JSON serde
    Records from cloudtrail are json objects wrapped in an object with key named "Records"
    This reader reads the line, and extract and serve the record through linebuffer one at a time
 */
public class CloudTrailLineReader
        implements LineReader
{
    private static final Logger log = Logger.get(CloudTrailLineReader.class);
    private static final String DOCUMENT_LIST_KEY = "Records";
    private static final String AS_STRING_SUFFIX = "_as_string";
    // Nested fields that are returned as String regardless of their nested schemas
    private static final Set<String> NESTED_FIELDS_WITHOUT_SCHEMA = ImmutableSet.of(
            "requestParameters",
            "responseElements",
            "additionalEventData",
            "serviceEventDetails");
    // Fields that we will append an additional String field with a suffix
    private static final Set<String> AS_STRING_FIELDS = ImmutableSet.of(
            "userIdentity");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final LineReader delegate;
    private Iterator<JsonNode> records;
    private LineBuffer fileLineBuffer;

    public CloudTrailLineReader(LineReader delegate, Supplier<LineBuffer> lineBufferSupplier)
            throws IOException
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        fileLineBuffer = lineBufferSupplier.get();
    }

    @Override
    public boolean isClosed()
    {
        return delegate.isClosed();
    }

    @Override
    public long getRetainedSize()
    {
        return delegate.getRetainedSize();
    }

    @Override
    public long getBytesRead()
    {
        return delegate.getBytesRead();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean readLine(LineBuffer lineBuffer)
            throws IOException
    {
        lineBuffer.reset();

        if (records != null && records.hasNext()) {
            lineBuffer.write(transformJsonNode(records.next()));
            return true;
        }

        while (delegate.readLine(fileLineBuffer)) {
            try {
                JsonNode node = MAPPER.readValue(fileLineBuffer.getBuffer(), JsonNode.class);
                records = node.get(DOCUMENT_LIST_KEY).elements();
            }
            catch (Exception e) {
                // We do not throw error in order to fully read cloudtrail records and ignored backward incompatible records
                log.error(e, "Encountered an exception while parsing CloudTrail records");
                continue;
            }

            if (records.hasNext()) {
                lineBuffer.write(transformJsonNode(records.next()));
                return true;
            }
        }

        // If there is no record left and not line left from the split, cleanup the resources
        close();
        return false;
    }

    @Override
    public void close()
            throws IOException
    {
        fileLineBuffer = null;
        delegate.close();
    }

    private byte[] transformJsonNode(JsonNode node)
    {
        ObjectNode objectNode = (ObjectNode) node;
        for (String field : NESTED_FIELDS_WITHOUT_SCHEMA) {
            if (node.has(field)) {
                JsonNode fieldNode = node.get(field);
                if (fieldNode.isNull()) {
                    objectNode.remove(field);
                }
                else {
                    objectNode.put(field, fieldNode.toString());
                }
            }
        }

        for (String field : AS_STRING_FIELDS) {
            if (node.has(field)) {
                JsonNode fieldNode = node.get(field);
                if (fieldNode.isNull()) {
                    objectNode.remove(field);
                }
                else {
                    objectNode.put(field + AS_STRING_SUFFIX, fieldNode.toString());
                }
            }
        }
        return objectNode.toString().getBytes(UTF_8);
    }
}
