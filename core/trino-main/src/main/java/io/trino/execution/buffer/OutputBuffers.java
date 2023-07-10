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
package io.trino.execution.buffer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PipelinedOutputBuffers.class, name = "pipelined"),
        @JsonSubTypes.Type(value = SpoolingOutputBuffers.class, name = "spool"),
})
public abstract class OutputBuffers
{
    private final long version;

    protected OutputBuffers(long version)
    {
        this.version = version;
    }

    public abstract void checkValidTransition(OutputBuffers newOutputBuffers);

    @JsonProperty
    public long getVersion()
    {
        return version;
    }
}
