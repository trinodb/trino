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
package io.trino.sql.planner.planprinter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JsonDistributedPlanFragments
{
    private final List<JsonDistributedFragment> jsonDistributedFragments;

    @JsonCreator
    public JsonDistributedPlanFragments(@JsonProperty("fragments") List<JsonDistributedFragment> jsonDistributedFragments)
    {
        this.jsonDistributedFragments = requireNonNull(jsonDistributedFragments, "jsonDistributedNode is null");
    }

    @JsonProperty
    public List<JsonDistributedFragment> getJsonDistributedFragments()
    {
        return jsonDistributedFragments;
    }
}
