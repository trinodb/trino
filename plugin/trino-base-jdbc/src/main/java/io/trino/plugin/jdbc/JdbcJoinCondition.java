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
package io.trino.plugin.jdbc;

import io.trino.spi.connector.JoinCondition.Operator;

import static java.util.Objects.requireNonNull;

public class JdbcJoinCondition
{
    private final JdbcColumnHandle leftColumn;
    private final Operator operator;
    private final JdbcColumnHandle rightColumn;

    public JdbcJoinCondition(JdbcColumnHandle leftColumn, Operator operator, JdbcColumnHandle rightColumn)
    {
        this.leftColumn = requireNonNull(leftColumn, "leftColumn is null");
        this.operator = requireNonNull(operator, "operator is null");
        this.rightColumn = requireNonNull(rightColumn, "rightColumn is null");
    }

    public JdbcColumnHandle getLeftColumn()
    {
        return leftColumn;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public JdbcColumnHandle getRightColumn()
    {
        return rightColumn;
    }
}
