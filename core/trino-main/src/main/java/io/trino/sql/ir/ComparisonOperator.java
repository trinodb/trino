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

public enum ComparisonOperator
{
    EQUAL("="),
    NOT_EQUAL("<>"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    IDENTICAL("≡");

    private final String value;

    ComparisonOperator(String value)
    {
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    public ComparisonOperator flip()
    {
        return switch (this) {
            case EQUAL -> EQUAL;
            case NOT_EQUAL -> NOT_EQUAL;
            case LESS_THAN -> GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
            case IDENTICAL -> IDENTICAL;
        };
    }

    public ComparisonOperator negate()
    {
        return switch (this) {
            case EQUAL -> NOT_EQUAL;
            case NOT_EQUAL -> EQUAL;
            case LESS_THAN -> GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL -> GREATER_THAN;
            case GREATER_THAN -> LESS_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL -> LESS_THAN;
            case IDENTICAL -> throw new IllegalArgumentException("Cannot negate: " + this);
        };
    }
}
