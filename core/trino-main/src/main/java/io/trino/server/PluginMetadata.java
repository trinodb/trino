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
package io.trino.server;

import java.util.List;

public final class PluginMetadata
{
    private final String name;
    private final List<String> connectors;
    private final List<String> functions;
    private final List<String> eventListeners;

    public PluginMetadata(String name, List<String> connectors, List<String> functions, List<String> eventListeners)
    {
        this.name = name;
        this.connectors = connectors;
        this.functions = functions;
        this.eventListeners = eventListeners;
    }

    public String getName()
    {
        return name;
    }

    public List<String> getConnectors()
    {
        return connectors;
    }

    public List<String> getFunctions()
    {
        return functions;
    }

    public List<String> getEventListeners()
    {
        return eventListeners;
    }
}
