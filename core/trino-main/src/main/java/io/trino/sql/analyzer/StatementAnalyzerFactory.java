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
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.SqlParser;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.transaction.TransactionManager;

import static java.util.Objects.requireNonNull;

public class StatementAnalyzerFactory
{
    private final PlannerContext plannerContext;
    private final SqlParser sqlParser;
    private final SessionTimeProvider sessionTimeProvider;
    private final AccessControl accessControl;
    private final TransactionManager transactionManager;
    private final GroupProvider groupProvider;
    private final TableProceduresRegistry tableProceduresRegistry;
    private final TableFunctionRegistry tableFunctionRegistry;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TableProceduresPropertyManager tableProceduresPropertyManager;

    @Inject
    public StatementAnalyzerFactory(
            PlannerContext plannerContext,
            SqlParser sqlParser,
            SessionTimeProvider sessionTimeProvider,
            AccessControl accessControl,
            TransactionManager transactionManager,
            GroupProvider groupProvider,
            TableProceduresRegistry tableProceduresRegistry,
            TableFunctionRegistry tableFunctionRegistry,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TableProceduresPropertyManager tableProceduresPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.sessionTimeProvider = requireNonNull(sessionTimeProvider, "sessionTimeProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.tableProceduresRegistry = requireNonNull(tableProceduresRegistry, "tableProceduresRegistry is null");
        this.tableFunctionRegistry = requireNonNull(tableFunctionRegistry, "tableFunctionRegistry is null");
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
                sessionTimeProvider,
                accessControl,
                transactionManager,
                groupProvider,
                tableProceduresRegistry,
                tableFunctionRegistry,
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
                sessionTimeProvider,
                groupProvider,
                accessControl,
                transactionManager,
                session,
                tableProceduresRegistry,
                tableFunctionRegistry,
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
                SessionTimeProvider.DEFAULT,
                accessControl,
                new NoOpTransactionManager(),
                user -> ImmutableSet.of(),
                new TableProceduresRegistry(CatalogServiceProvider.fail("procedures are not supported in testing analyzer")),
                new TableFunctionRegistry(CatalogServiceProvider.fail("table functions are not supported in testing analyzer")),
                new SessionPropertyManager(),
                tablePropertyManager,
                analyzePropertyManager,
                new TableProceduresPropertyManager(CatalogServiceProvider.fail("procedures are not supported in testing analyzer")));
    }
}
