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
package io.trino.sql.rewrite;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class StatementRewrite
{
    private static final List<Rewrite> REWRITES = ImmutableList.of(
            new DescribeInputRewrite(),
            new DescribeOutputRewrite(),
            new ShowQueriesRewrite(),
            new ShowStatsRewrite(),
            new ExplainRewrite());

    private StatementRewrite() {}

    public static Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            GroupProvider groupProvider,
            AccessControl accessControl,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator)
    {
        for (Rewrite rewrite : REWRITES) {
            node = requireNonNull(rewrite.rewrite(session, metadata, parser, queryExplainer, node, parameters, parameterLookup, groupProvider, accessControl, warningCollector, statsCalculator), "Statement rewrite returned null");
        }
        return node;
    }

    interface Rewrite
    {
        Statement rewrite(
                Session session,
                Metadata metadata,
                SqlParser parser,
                Optional<QueryExplainer> queryExplainer,
                Statement node,
                List<Expression> parameters,
                Map<NodeRef<Parameter>, Expression> parameterLookup,
                GroupProvider groupProvider,
                AccessControl accessControl,
                WarningCollector warningCollector,
                StatsCalculator statsCalculator);
    }
}
