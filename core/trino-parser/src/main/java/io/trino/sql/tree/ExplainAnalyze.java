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
    private final Optional<ExplainAnalyzeOption> option;

    public ExplainAnalyze(Statement statement, boolean verbose)
    {
        this(Optional.empty(), statement, verbose, Optional.empty());
    }

    public ExplainAnalyze(Statement statement, boolean verbose, Optional<ExplainAnalyzeOption> option)
    {
        this(Optional.empty(), statement, verbose, option);
    }

    public ExplainAnalyze(NodeLocation location, boolean verbose, Statement statement, Optional<ExplainAnalyzeOption> option)
    {
        this(Optional.of(location), statement, verbose, option);
    }

    public ExplainAnalyze(Optional<NodeLocation> location, Statement statement, boolean verbose, Optional<ExplainAnalyzeOption> option)
    {
        super(location);
        this.statement = requireNonNull(statement, "statement is null");
        this.verbose = verbose;
        this.option = option;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    public Optional<ExplainAnalyzeOption> getOption()
    {
        return option;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplainAnalyze(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(statement);
        option.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statement, verbose, option);
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
        return Objects.equals(statement, o.statement) && Objects.equals(option, o.option);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statement", statement)
                .add("verbose", verbose)
                .add("option", option)
                .toString();
    }
}
