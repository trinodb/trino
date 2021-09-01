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
package io.trino.plugin.pulsar.decoder.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Splitter;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.json.JsonFieldDecoder;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.plugin.pulsar.util.ObjectMapperFactory;
import io.trino.spi.TrinoException;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.node.MissingNode;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Pulsar {@link org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType#JSON} RowDecoder.
 */
public class PulsarJsonRowDecoder
        implements PulsarRowDecoder
{
    private final Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders;

    private final GenericJsonSchema genericJsonSchema;

    public PulsarJsonRowDecoder(GenericJsonSchema genericJsonSchema,
                                Set<DecoderColumnHandle> columns)
    {
        this.genericJsonSchema = requireNonNull(genericJsonSchema, "genericJsonSchema is null");
        this.fieldDecoders = columns.stream().collect(toImmutableMap(identity(), PulsarJsonFieldDecoder::new));
    }

    private static JsonNode locateNode(JsonNode tree, DecoderColumnHandle columnHandle)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());
        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }

    /**
     * decode ByteBuf by {@link org.apache.pulsar.client.api.schema.GenericSchema}.
     *
     * @param byteBuf
     * @return
     */
    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf)
    {
        GenericJsonRecord record = (GenericJsonRecord) genericJsonSchema.decode(byteBuf);
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();
        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            com.fasterxml.jackson.databind.JsonNode node = null;
            try {
                node = ObjectMapperFactory.toOriginalJsonNode(locateNode(record.getJsonNode(), columnHandle));
            }
            catch (JsonProcessingException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Decoding json record failed.", e);
            }
            decodedRow.put(columnHandle, decoder.decode(node));
        }
        return Optional.of(decodedRow);
    }
}
