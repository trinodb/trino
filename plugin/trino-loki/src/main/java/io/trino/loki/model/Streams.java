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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;

@JsonDeserialize(using = StreamsDeserializer.class)
public final class Streams
        extends QueryResult.Result
{

    public List<Stream> getStreams()
    {
        return streams;
    }

    public void setStreams(List<Stream> streams)
    {
        this.streams = streams;
    }

    private List<Stream> streams;

    public record Stream(
            @JsonProperty("stream")
            Map<String, String> labels,
            List<LogEntry> values
    ) {}
}
