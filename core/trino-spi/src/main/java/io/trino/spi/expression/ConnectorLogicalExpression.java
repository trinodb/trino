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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class ConnectorLogicalExpression
        extends ConnectorExpression
{
    private String operator;
    private List<? extends ConnectorExpression> terms;

    public ConnectorLogicalExpression(String operator, List<? extends ConnectorExpression> terms)
    {
        super(BOOLEAN);
        this.operator = requireNonNull(operator, "operator is null");
        this.terms = requireNonNull(terms, "terms is null");
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return terms;
    }

    public List<? extends ConnectorExpression> getTerms()
    {
        return terms;
    }

    public String getOperator()
    {
        return operator;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, terms);
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
        ConnectorLogicalExpression logical = (ConnectorLogicalExpression) o;
        return Objects.equals(getType(), logical.getType()) &&
                Objects.equals(getOperator(), logical.getOperator()) &&
                Objects.equals(getTerms(), logical.getTerms());
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", ConnectorLogicalExpression.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("operator=" + getOperator())
                .add("terms=" + terms)
                .toString();
    }
}
