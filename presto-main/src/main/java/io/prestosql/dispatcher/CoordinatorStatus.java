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
package io.prestosql.dispatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.NodeState;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CoordinatorStatus
{
    private final NodeState nodeState;
    private final List<BasicQueryInfo> queries;

    @JsonCreator
    public CoordinatorStatus(
            @JsonProperty("nodeState") NodeState nodeState,
            @JsonProperty("queries") List<BasicQueryInfo> queries)
    {
        this.nodeState = requireNonNull(nodeState, "nodeState is null");
        this.queries = ImmutableList.copyOf(requireNonNull(queries, "queries is null"));
    }

    @JsonProperty
    public NodeState getNodeState()
    {
        return nodeState;
    }

    @JsonProperty
    public List<BasicQueryInfo> getQueries()
    {
        return queries;
    }
}
