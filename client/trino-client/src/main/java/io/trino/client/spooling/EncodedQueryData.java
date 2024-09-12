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
package io.trino.client.spooling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.client.QueryData;
import io.trino.client.QueryDataDecoder;
import io.trino.client.RawQueryData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EncodedQueryData
        implements QueryData
{
    private final String encoding;
    private final DataAttributes metadata;
    private final List<Segment> segments;

    @JsonCreator
    public EncodedQueryData(
            @JsonProperty("encoding") String encoding,
            @JsonProperty("metadata") Map<String, Object> metadata,
            @JsonProperty("segments") List<Segment> segments)
    {
        this(encoding, new DataAttributes(metadata), segments);
    }

    public EncodedQueryData(String encoding, DataAttributes metadata, List<Segment> segments)
    {
        this.encoding = requireNonNull(encoding, "encoding is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.segments = ImmutableList.copyOf(requireNonNull(segments, "segments is null"));
    }

    @JsonProperty("segments")
    public List<Segment> getSegments()
    {
        return segments;
    }

    @JsonProperty("encoding")
    public String getEncoding()
    {
        return encoding;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("metadata")
    public Map<String, Object> getJsonMetadata()
    {
        return metadata.attributes;
    }

    @JsonIgnore
    public DataAttributes getMetadata()
    {
        return metadata;
    }

    @Override
    public Iterable<List<Object>> getData()
    {
        throw new UnsupportedOperationException("EncodedQueryData required decoding via matching QueryDataDecoder");
    }

    public QueryData toRawData(QueryDataDecoder decoder, SegmentLoader segmentLoader)
    {
        if (!decoder.encoding().equals(encoding)) {
            throw new IllegalArgumentException(format("Invalid decoder supplied, expected %s, got %s", encoding, decoder.encoding()));
        }

        return RawQueryData.of(unmodifiableIterable(concat(transform(segments, segment -> {
            if (segment instanceof InlineSegment) {
                InlineSegment inline = (InlineSegment) segment;
                try {
                    return decoder.decode(new ByteArrayInputStream(inline.getData()), inline.getMetadata());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            if (segment instanceof SpooledSegment) {
                SpooledSegment spooled = (SpooledSegment) segment;
                try (InputStream stream = segmentLoader.load(spooled)) {
                    return decoder.decode(stream, segment.getMetadata());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            throw new IllegalArgumentException("Unexpected segment type: " + segment.getClass().getSimpleName());
        }))));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("encoding", encoding)
                .add("segments", segments)
                .add("metadata", metadata.attributes.keySet())
                .toString();
    }

    public static Builder builder(String format)
    {
        return new Builder(format);
    }

    public static class Builder
    {
        private final String encoding;
        private final ImmutableList.Builder<Segment> segments = ImmutableList.builder();
        private DataAttributes metadata = DataAttributes.empty();

        private Builder(String encoding)
        {
            this.encoding = requireNonNull(encoding, "encoding is null");
        }

        public Builder withSegment(Segment segment)
        {
            this.segments.add(segment);
            return this;
        }

        public Builder withAttributes(DataAttributes attributes)
        {
            this.metadata = requireNonNull(attributes, "attributes is null");
            return this;
        }

        public EncodedQueryData build()
        {
            return new EncodedQueryData(encoding, metadata, segments.build());
        }
    }
}
