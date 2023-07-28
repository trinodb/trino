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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class EncodedQueryData
        implements QueryData
{
    private final List<QueryDataPart> parts;
    private final QueryDataEncodings encoding;

    @JsonCreator
    public EncodedQueryData(@JsonProperty("parts") List<QueryDataPart> parts, @JsonProperty("encoding") QueryDataEncodings encoding)
    {
        this.parts = ImmutableList.copyOf(requireNonNull(parts, "parts is null"));
        this.encoding = requireNonNull(encoding, "encoding is null").enforceSingleEncoding();
    }

    @JsonProperty("parts")
    public List<QueryDataPart> getParts()
    {
        return parts;
    }

    @JsonProperty("encoding")
    public QueryDataEncodings getEncoding()
    {
        return encoding;
    }

    @Override
    public Iterable<List<Object>> getData()
    {
        throw new UnsupportedOperationException("EncodedQueryData needs a decoding before enumerating values");
    }

    public DataSize getDataSize()
    {
        return DataSize.ofBytes(parts.stream()
                        .reduce(0L, (sum, part) -> sum + part.getSize().toBytes(), Long::sum))
                .succinct();
    }

    public long getTotalRowCount()
    {
        return parts.stream().reduce(0L, (sum, part) -> sum + part.getRowCount(), Long::sum);
    }

    public static Builder builder(QueryDataEncodings format)
    {
        return new Builder(format);
    }

    public EncodedQueryData mergeWith(EncodedQueryData another)
    {
        verify(getEncoding().equals(another.getEncoding()), "Merging two different formats is not supported");
        return builder(getEncoding())
                .addParts(getParts())
                .addParts(another.getParts())
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("parts", parts)
                .add("totalSize", getDataSize())
                .add("totalRows", getTotalRowCount())
                .add("encoding", getEncoding())
                .toString();
    }

    public static class Builder
    {
        private final QueryDataEncodings encoding;
        private final ImmutableList.Builder<QueryDataPart> parts = ImmutableList.builder();

        private Builder(QueryDataEncodings encoding)
        {
            this.encoding = requireNonNull(encoding, "encoding is null").enforceSingleEncoding();
        }

        public Builder addPart(QueryDataPart part)
        {
            this.parts.add(part);
            return this;
        }

        public Builder addParts(List<QueryDataPart> parts)
        {
            this.parts.addAll(parts);
            return this;
        }

        public EncodedQueryData build()
        {
            return new EncodedQueryData(parts.build(), encoding);
        }
    }
}
