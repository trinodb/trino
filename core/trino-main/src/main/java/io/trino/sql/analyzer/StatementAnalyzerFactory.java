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
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.SqlParser;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class StatementAnalyzerFactory
{
    private final PlannerContext plannerContext;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final TableProceduresRegistry tableProceduresRegistry;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TableProceduresPropertyManager tableProceduresPropertyManager;

    @Inject
    public StatementAnalyzerFactory(
            PlannerContext plannerContext,
            SqlParser sqlParser,
            AccessControl accessControl,
            TableProceduresRegistry tableProceduresRegistry,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TableProceduresPropertyManager tableProceduresPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.tableProceduresRegistry = requireNonNull(tableProceduresRegistry, "tableProceduresRegistry is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.analyzePropertyManager = requireNonNull(analyzePropertyManager, "analyzePropertyManager is null");
        this.tableProceduresPropertyManager = requireNonNull(tableProceduresPropertyManager, "tableProceduresPropertyManager is null");
    }

    public StatementAnalyzerFactory withSpecializedAccessControl(AccessControl accessControl)
    {
        return new StatementAnalyzerFactory(
                plannerContext,
                sqlParser,
                accessControl,
                tableProceduresRegistry,
                sessionPropertyManager,
                tablePropertyManager,
                analyzePropertyManager,
                tableProceduresPropertyManager);
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
                plannerContext,
                sqlParser,
                accessControl,
                session,
                tableProceduresRegistry,
                sessionPropertyManager,
                tablePropertyManager,
                analyzePropertyManager,
                tableProceduresPropertyManager,
                warningCollector,
                correlationSupport);
    }

    public static StatementAnalyzerFactory createTestingStatementAnalyzerFactory(
            PlannerContext plannerContext,
            AccessControl accessControl,
            TablePropertyManager tablePropertyManager,
            AnalyzePropertyManager analyzePropertyManager)
    {
        return new StatementAnalyzerFactory(
                plannerContext,
                new SqlParser(),
                accessControl,
                new TableProceduresRegistry(),
                new SessionPropertyManager(),
                tablePropertyManager,
                analyzePropertyManager,
                new TableProceduresPropertyManager());
    }
}
