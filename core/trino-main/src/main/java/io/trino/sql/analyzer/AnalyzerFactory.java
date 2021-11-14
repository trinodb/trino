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
package io.trino.sql.analyzer;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AnalyzerFactory
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;
    private final StatementRewrite statementRewrite;

    @Inject
    public AnalyzerFactory(
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            GroupProvider groupProvider,
            StatementRewrite statementRewrite)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.statementRewrite = requireNonNull(statementRewrite, "statementRewrite is null");
    }

    public Analyzer createAnalyzer(
            Session session,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector)
    {
        return new Analyzer(
                session,
                metadata,
                sqlParser,
                this,
                groupProvider,
                accessControl,
                parameters,
                parameterLookup,
                warningCollector,
                statementRewrite);
    }
}
