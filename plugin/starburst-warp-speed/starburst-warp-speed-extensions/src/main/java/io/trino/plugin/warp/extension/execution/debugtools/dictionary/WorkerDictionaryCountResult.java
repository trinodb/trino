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
package io.trino.plugin.warp.extension.execution.debugtools.dictionary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.dictionary.DebugDictionaryMetadata;

import java.util.List;

public class WorkerDictionaryCountResult
{
    private final List<DebugDictionaryMetadata> debugDictionaryMetadataList;
    private final String nodeIdentifier;

    @JsonCreator
    public WorkerDictionaryCountResult(@JsonProperty("dictionaryMetadataList") List<DebugDictionaryMetadata> debugDictionaryMetadataList,
            @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("nodeIdentifier") String nodeIdentifier)
    {
        this.debugDictionaryMetadataList = debugDictionaryMetadataList;
        this.nodeIdentifier = nodeIdentifier;
    }

    @JsonProperty
    public List<DebugDictionaryMetadata> getDictionaryMetadataList()
    {
        return debugDictionaryMetadataList;
    }

    @JsonProperty
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String toString()
    {
        return "WorkerDictionaryCountResult{" +
                "dictionaryMetadata=" + debugDictionaryMetadataList +
                ", nodeIdentifier='" + nodeIdentifier + '\'' +
                '}';
    }
}
