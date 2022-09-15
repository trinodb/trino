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

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QueryPlanNodeInfo
{
    private final String id;
    private final String name;
    private final Map<String, String> descriptor;
    private final List<String> details;
    private final List<QueryPlanNodeInfo> children;

    @JsonCreator
    public QueryPlanNodeInfo(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("descriptor") Map<String, String> descriptor,
            @JsonProperty("details") List<String> details,
            @JsonProperty("children") List<QueryPlanNodeInfo> children)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.details = requireNonNull(details, "details is null");
        this.children = requireNonNull(children, "children is null");
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Map<String, String> getDescriptor()
    {
        return descriptor;
    }

    @JsonProperty
    public List<String> getDetails()
    {
        return details;
    }

    @JsonProperty
    public List<QueryPlanNodeInfo> getChildren()
    {
        return children;
    }
}
