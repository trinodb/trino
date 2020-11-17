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
package io.prestosql.plugin.kafka.schemareader;

import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.plugin.kafka.KafkaTableHandle;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DispatchingContentSchemaReader
        implements ContentSchemaReader
{
    private final Map<String, ContentSchemaReader> contentSchemaReaders;

    @Inject
    public DispatchingContentSchemaReader(Map<String, ContentSchemaReader> contentSchemaReaders)
    {
        requireNonNull(contentSchemaReaders, "schemaReaders is null");
        this.contentSchemaReaders = ImmutableMap.copyOf(contentSchemaReaders);
    }

    @Override
    public Optional<String> readKeyContentSchema(KafkaTableHandle tableHandle)
    {
        return getSchemaReader(tableHandle.getKeyDataFormat(), tableHandle.getKeyDecoderParams())
                .flatMap(schemaReader -> schemaReader.readKeyContentSchema(tableHandle));
    }

    @Override
    public Optional<String> readValueContentSchema(KafkaTableHandle tableHandle)
    {
        return getSchemaReader(tableHandle.getMessageDataFormat(), tableHandle.getMessageDecoderParams())
                .flatMap(schemaReader -> schemaReader.readValueContentSchema(tableHandle));
    }

    private Optional<ContentSchemaReader> getSchemaReader(String dataFormat, Optional<Map<String, String>> decoderParams)
    {
        // For backwards compatibility
        if (dataFormat.equals(DummyRowDecoder.NAME)) {
            return Optional.empty();
        }
        // For backwards compatibility
        String schemaReaderName = decoderParams.flatMap(params -> Optional.ofNullable(params.get(SCHEMA_READER))).orElse(DefaultContentSchemaReader.NAME);

        ContentSchemaReader contentSchemaReader = contentSchemaReaders.get(schemaReaderName);
        checkState(contentSchemaReader != null, "contentSchemaReader '%s' not found", schemaReaderName);
        return Optional.of(contentSchemaReader);
    }
}
