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

public record WhenClause(Expression operand, Expression result)
{
    @JsonCreator
    public WhenClause(@JsonProperty("operand") Expression operand, @JsonProperty("result") Expression result)
    {
        this.operand = operand;
        this.result = result;
    }

    @Override
    public String toString()
    {
        return "When(%s, %s)".formatted(operand, result);
    }
}
