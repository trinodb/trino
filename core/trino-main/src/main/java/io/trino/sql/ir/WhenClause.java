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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class WhenClause
{
    private final Expression operand;
    private final Expression result;

    @JsonCreator
    public WhenClause(Expression operand, Expression result)
    {
        this.operand = operand;
        this.result = result;
    }

    @JsonProperty
    public Expression getOperand()
    {
        return operand;
    }

    @JsonProperty
    public Expression getResult()
    {
        return result;
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

        WhenClause that = (WhenClause) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(result, that.result);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operand, result);
    }

    @Override
    public String toString()
    {
        return "When(%s, %s)".formatted(operand, result);
    }
}
