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
package io.trino.spi.expression;

import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

public class VarArgs
        extends ConnectorExpression
{
    private final List<ConnectorExpression> items;

    public VarArgs(List<ConnectorExpression> items, List<Type> types)
    {
        super(RowType.anonymous(types));
        this.items = List.copyOf(items);
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return items;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(items, getType());
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

        VarArgs that = (VarArgs) o;
        return Objects.equals(items, that.items) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return "row(" + String.join(", ", items.stream().map(ConnectorExpression::toString).toList()) + ")";
    }
}
