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
package io.trino.execution.scheduler;

import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class SimpleNodeProvider
{
    private final List<InternalNode> sortedNodes;

    public SimpleNodeProvider(List<InternalNode> sortedNodes)
    {
        this.sortedNodes = sortedNodes;
    }

    public List<HostAddress> get(String identifier, int count)
    {
        int size = sortedNodes.size();
        int mod = identifier.hashCode() % size;
        int position = mod < 0 ? mod + size : mod;
        List<HostAddress> chosenCandidates = new ArrayList<>();
        if (count > size) {
            count = size;
        }
        for (int i = 0; i < count && i < sortedNodes.size(); i++) {
            chosenCandidates.add(sortedNodes.get((position + i) % size).getHostAndPort());
        }
        return unmodifiableList(chosenCandidates);
    }
}
