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
package io.trino.plugin.base.expression;

import io.trino.spi.expression.ConnectorExpression;

import static java.util.Objects.requireNonNull;

public class ConnectorExpressionWithIndex
{
    private final ConnectorExpression connectorExpression;

    private final int index;

    private final boolean isLast;

    public ConnectorExpressionWithIndex(ConnectorExpression connectorExpression, int index, boolean isLast)
    {
        this.connectorExpression = requireNonNull(connectorExpression, "connectorExpression is null");
        this.index = index;
        this.isLast = isLast;
    }

    public ConnectorExpression getConnectorExpression()
    {
        return connectorExpression;
    }

    public int getIndex()
    {
        return index;
    }

    public boolean isLast()
    {
        return isLast;
    }
}
