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
package io.trino.plugin.varada.dispatcher.query.data.match;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class LogicalMatchData
        implements MatchData
{
    private final Operator operator;
    private final List<MatchData> terms;
    private final List<QueryMatchData> leaves;

    public LogicalMatchData(Operator operator, List<MatchData> terms)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.terms = requireNonNull(terms, "terms is null");
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("terms is empty");
        }
        this.leaves = terms.stream()
                .flatMap(term -> term.getLeavesDFS().stream())
                .toList();
    }

    public Operator getOperator()
    {
        return operator;
    }

    public List<MatchData> getTerms()
    {
        return terms;
    }

    @Override
    public boolean isPartOfLogicalOr()
    {
        return operator == Operator.OR;
    }

    @Override
    public List<QueryMatchData> getLeavesDFS()
    {
        return leaves;
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
        LogicalMatchData that = (LogicalMatchData) o;
        return operator == that.operator &&
                Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, terms);
    }

    @Override
    public String toString()
    {
        return "LogicalMatchData{" +
                "operator=" + operator +
                ", terms=" + terms +
                '}';
    }

    public enum Operator
    {
        AND,
        OR
    }
}
