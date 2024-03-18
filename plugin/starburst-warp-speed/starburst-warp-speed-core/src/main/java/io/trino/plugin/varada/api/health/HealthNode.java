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
package io.trino.plugin.varada.api.health;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record HealthNode(
        long createEpochTime,
        String nodeIdentifier,
        String httpUri, String state)
{
    @JsonCreator
    public HealthNode(@JsonProperty("createEpochTime") long createEpochTime,
            @JsonProperty("nodeIdentifier") String nodeIdentifier,
            @JsonProperty("httpUri") String httpUri,
            @JsonProperty("state") String state)
    {
        this.createEpochTime = createEpochTime;
        this.nodeIdentifier = nodeIdentifier;
        this.httpUri = httpUri;
        this.state = state;
    }
}
