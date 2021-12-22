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
package io.trino.sql.parser.hive;

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;

import java.util.List;
import java.util.Optional;

public class StarExpression
        extends Expression
{
    private List<Identifier> identifiers;

    public StarExpression(NodeLocation location, List<Identifier> identifiers)
    {
        super(Optional.of(location));
        this.identifiers = identifiers;
    }

    public List<Identifier> getIdentifiers()
    {
        return identifiers;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return true;
    }
}
