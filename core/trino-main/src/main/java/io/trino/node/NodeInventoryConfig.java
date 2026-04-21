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
package io.trino.node;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class NodeInventoryConfig
{
    public enum NodeInventoryType
    {
        ANNOUNCE,
        DNS,
        AIRLIFT_DISCOVERY,
    }

    private NodeInventoryType type = NodeInventoryType.ANNOUNCE;

    @NotNull
    public NodeInventoryType getType()
    {
        return type;
    }

    @Config("discovery.type")
    public NodeInventoryConfig setType(NodeInventoryType type)
    {
        this.type = type;
        return this;
    }
}
