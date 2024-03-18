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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.warp.gen.constants.MatchNodeType;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class LogicalMatchNode
        implements MatchNode
{
    private final MatchNodeType nodeType;
    private final List<MatchNode> terms;

    public LogicalMatchNode(MatchNodeType nodeType, List<MatchNode> terms)
    {
        this.nodeType = nodeType;
        checkArgument(nodeType == MatchNodeType.MATCH_NODE_TYPE_OR || nodeType == MatchNodeType.MATCH_NODE_TYPE_AND);
        this.terms = terms;
    }

    @Override
    public MatchNodeType getNodeType()
    {
        return nodeType;
    }

    @Override
    public List<MatchNode> getChildren()
    {
        return terms;
    }

    @Override
    public int getDumpSize()
    {
        // 1 is for the MatchNodeType and 1 for the num of the children
        return 2 + terms.stream().mapToInt(MatchNode::getDumpSize).sum();
    }

    @Override
    public int dump(int[] output, int offset)
    {
        output[offset++] = getNodeType().ordinal();
        output[offset++] = terms.size();
        for (MatchNode term : terms) {
            offset = term.dump(output, offset);
        }
        return offset;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalMatchNode that = (LogicalMatchNode) o;
        return nodeType == that.nodeType &&
                Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeType, terms);
    }

    @Override
    public String toString()
    {
        return "LogicalMatchNode{" +
                "nodeType=" + nodeType +
                ", terms=" + terms +
                '}';
    }
}
