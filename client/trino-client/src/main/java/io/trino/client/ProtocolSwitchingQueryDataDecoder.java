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
package io.trino.client;

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.QueryDataEncodings.multipleEncodings;
import static io.trino.client.QueryDataEncodings.singleEncoding;
import static io.trino.client.QueryDataReference.INLINE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ProtocolSwitchingQueryDataDecoder
        implements QueryDataDecoder
{
    private final QueryDataDecoder legacyDecoder;
    private final List<EncodingDecoder> decoders;

    private ProtocolSwitchingQueryDataDecoder(QueryDataDecoder legacyDecoder, List<EncodingDecoder> decoders)
    {
        this.legacyDecoder = requireNonNull(legacyDecoder, "legacyDecoder is null");
        this.decoders = requireNonNull(decoders, "decoders is null");
    }

    @Nullable
    @Override
    public Iterable<List<Object>> decode(@Nullable QueryData data, List<Column> columns)
    {
        if (data == null) {
            return null;
        }

        return findDecoder(data)
                .orElseThrow(() -> new IllegalStateException(format("Supported client encodings are %s but got %s", consumes(), data)))
                .decode(data, columns);
    }

    private Optional<QueryDataDecoder> findDecoder(QueryData data)
    {
        if (data instanceof LegacyQueryData) {
            return Optional.of(legacyDecoder);
        }

        verify(data instanceof EncodedQueryData, "Expected EncodedQueryData but got %s", data.getClass().getSimpleName());
        EncodedQueryData encodedQueryData = (EncodedQueryData) data;

        for (EncodingDecoder entry : decoders) {
            QueryDataDecoder decoder = entry.getDecoder();
            if (encodedQueryData.getEncoding().equals(decoder.consumes())) {
                return Optional.of(decoder);
            }
        }

        return Optional.empty();
    }

    @Override
    public QueryDataEncodings consumes()
    {
        return multipleEncodings(decoders.stream().map(EncodingDecoder::getEncoding).collect(toImmutableList()));
    }

    public static ProtocolSwitchingQueryDataDecoder.Builder negotiateProtocol()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableList.Builder<EncodingDecoder> encodingDecoders = ImmutableList.builder();

        private Builder() {}

        private QueryDataDecoder legacyQueryDataDeserializer = (data, columns) -> {
            throw new IllegalStateException("Fallback to the legacy protocol is not supported");
        };

        public Builder withInlineEncoding(QueryDataSerialization serialization)
        {
            QueryDataEncodings encoding = singleEncoding(INLINE, serialization);
            if (requireNonNull(serialization) == QueryDataSerialization.JSON) {
                return withSupport(encoding, new InlineQueryDataDecoder(new JsonQueryDataDeserializer()));
            }

            throw new IllegalArgumentException("Unknown serialization " + serialization);
        }

        public Builder withSupport(QueryDataEncodings encodings, QueryDataDecoder deserializer)
        {
            encodingDecoders.add(new EncodingDecoder(encodings, deserializer));
            return this;
        }

        public Builder withFallbackToLegacyProtocol()
        {
            legacyQueryDataDeserializer = new LegacyQueryDataDecoder();
            return this;
        }

        public ProtocolSwitchingQueryDataDecoder build()
        {
            return new ProtocolSwitchingQueryDataDecoder(legacyQueryDataDeserializer, encodingDecoders.build());
        }
    }

    private static class EncodingDecoder
    {
        protected final QueryDataEncodings encoding;
        protected final QueryDataDecoder decoder;

        private EncodingDecoder(QueryDataEncodings encoding, QueryDataDecoder decoder)
        {
            this.encoding = requireNonNull(encoding, "encoding is null").enforceSingleEncoding();
            this.decoder = requireNonNull(decoder, "decoder is null");
        }

        public QueryDataEncodings getEncoding()
        {
            return encoding;
        }

        public QueryDataDecoder getDecoder()
        {
            return decoder;
        }
    }
}
