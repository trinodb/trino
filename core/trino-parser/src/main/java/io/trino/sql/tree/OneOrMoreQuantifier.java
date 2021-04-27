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
package io.trino.sql.tree;

import java.util.Optional;

public class OneOrMoreQuantifier
        extends PatternQuantifier
{
    public OneOrMoreQuantifier(boolean greedy)
    {
        this(Optional.empty(), greedy);
    }

    public OneOrMoreQuantifier(NodeLocation location, boolean greedy)
    {
        this(Optional.of(location), greedy);
    }

    public OneOrMoreQuantifier(Optional<NodeLocation> location, boolean greedy)
    {
        super(location, greedy);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitOneOrMoreQuantifier(this, context);
    }
}
