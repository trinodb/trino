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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SpooledSegment
        extends Segment
{
    private final URI dataUri;
    private final URI ackUri;
    private final Map<String, List<String>> headers;

    @JsonCreator
    public SpooledSegment(
            @JsonProperty("uri") URI dataUri,
            @JsonProperty("ackUri") URI ackUri,
            @JsonProperty("metadata") DataAttributes metadata,
            @JsonProperty("headers") Map<String, List<String>> headers)
    {
        super(metadata);
        this.dataUri = requireNonNull(dataUri, "dataUri is null");
        this.ackUri = requireNonNull(ackUri, "ackUri is null");
        this.headers = firstNonNull(headers, ImmutableMap.of());
    }

    @JsonProperty("uri")
    public URI getDataUri()
    {
        return dataUri;
    }

    @JsonProperty("ackUri")
    public URI getAckUri()
    {
        return ackUri;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("headers")
    public Map<String, List<String>> getHeaders()
    {
        return headers;
    }

    @Override
    public String toString()
    {
        return format("SpooledSegment{offset=%d, rows=%d, size=%d, headers=%s}", getOffset(), getRowsCount(), getSegmentSize(), headers.keySet());
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpooledSegment that = (SpooledSegment) o;
        return Objects.equals(dataUri, that.dataUri)
                && Objects.equals(ackUri, that.ackUri)
                && Objects.equals(headers, that.headers)
                && Objects.equals(getMetadata(), that.getMetadata());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataUri, ackUri, headers, getMetadata());
    }
}
