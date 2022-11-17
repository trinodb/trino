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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.time.Instant;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryCreatedEvent
{
    private final Instant createTime;

    private final QueryContext context;
    private final QueryMetadata metadata;

    @JsonCreator
    @Unstable
    public QueryCreatedEvent(Instant createTime, QueryContext context, QueryMetadata metadata)
    {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.context = requireNonNull(context, "context is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @JsonProperty
    public Instant getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public QueryContext getContext()
    {
        return context;
    }

    @JsonProperty
    public QueryMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", QueryCreatedEvent.class.getSimpleName() + "[", "]")
                .add("queryId='" + getMetadata().getQueryId() + "'")
                .toString();
    }
}
