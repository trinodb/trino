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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.TypeProvider;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class StatementAnalyzerFactory
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;
    private final TableProceduresRegistry tableProceduresRegistry;

    @Inject
    public StatementAnalyzerFactory(
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            GroupProvider groupProvider,
            TableProceduresRegistry tableProceduresRegistry)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.tableProceduresRegistry = requireNonNull(tableProceduresRegistry, "tableProceduresRegistry is null");
    }

    public StatementAnalyzerFactory withSpecializedAccessControl(AccessControl accessControl)
    {
        return new StatementAnalyzerFactory(metadata, sqlParser, accessControl, groupProvider, tableProceduresRegistry);
    }

    public StatementAnalyzer createStatementAnalyzer(
            Analysis analysis,
            Session session,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        return new StatementAnalyzer(
                this,
                analysis,
                metadata,
                sqlParser,
                groupProvider,
                accessControl,
                session,
                tableProceduresRegistry,
                warningCollector,
                correlationSupport);
    }

    // this is only for the static factory methods on ExpressionAnalyzer, and should not be used for any other purpose
    ExpressionAnalyzer createExpressionAnalyzer(
            Analysis analysis,
            Session session,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        return new ExpressionAnalyzer(
                metadata,
                accessControl,
                (node, correlationSupport) -> createStatementAnalyzer(
                        analysis,
                        session,
                        warningCollector,
                        correlationSupport),
                session,
                types,
                analysis.getParameters(),
                warningCollector,
                analysis.isDescribe(),
                analysis::getType,
                analysis::getWindow);
    }

    public static StatementAnalyzerFactory createTestingStatementAnalyzerFactory(Metadata metadata, AccessControl accessControl)
    {
        return new StatementAnalyzerFactory(
                metadata,
                new SqlParser(),
                accessControl,
                user -> ImmutableSet.of(),
                new TableProceduresRegistry());
    }
}
