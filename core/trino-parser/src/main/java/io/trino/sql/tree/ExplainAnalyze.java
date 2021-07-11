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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ExplainAnalyze
        extends Statement
{
    private final Statement statement;
    private final boolean verbose;

    public ExplainAnalyze(Statement statement, boolean verbose)
    {
        this(Optional.empty(), statement, verbose);
    }

    public ExplainAnalyze(NodeLocation location, boolean verbose, Statement statement)
    {
        this(Optional.of(location), statement, verbose);
    }

    public ExplainAnalyze(Optional<NodeLocation> location, Statement statement, boolean verbose)
    {
        super(location);
        this.statement = requireNonNull(statement, "statement is null");
        this.verbose = verbose;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplainAnalyze(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(statement)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statement, verbose);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ExplainAnalyze o = (ExplainAnalyze) obj;
        return Objects.equals(statement, o.statement);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statement", statement)
                .add("verbose", verbose)
                .toString();
    }
}
