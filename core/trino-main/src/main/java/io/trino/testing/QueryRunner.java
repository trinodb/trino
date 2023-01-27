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
package io.trino.testing;

import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

public interface QueryRunner
        extends Closeable
{
    @Override
    void close();

    int getNodeCount();

    Session getDefaultSession();

    TransactionManager getTransactionManager();

    Metadata getMetadata();

    TypeManager getTypeManager();

    QueryExplainer getQueryExplainer();

    SessionPropertyManager getSessionPropertyManager();

    FunctionManager getFunctionManager();

    SplitManager getSplitManager();

    ExchangeManager getExchangeManager();

    PageSourceManager getPageSourceManager();

    NodePartitioningManager getNodePartitioningManager();

    StatsCalculator getStatsCalculator();

    TestingGroupProviderManager getGroupProvider();

    TestingAccessControlManager getAccessControl();

    MaterializedResult execute(@Language("SQL") String sql);

    MaterializedResult execute(Session session, @Language("SQL") String sql);

    default MaterializedResultWithPlan executeWithPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        throw new UnsupportedOperationException();
    }

    default Plan createPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        throw new UnsupportedOperationException();
    }

    List<QualifiedObjectName> listTables(Session session, String catalog, String schema);

    boolean tableExists(Session session, String table);

    void installPlugin(Plugin plugin);

    void addFunctions(FunctionBundle functionBundle);

    void createCatalog(String catalogName, String connectorName, Map<String, String> properties);

    Lock getExclusiveLock();

    void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            InjectedFailureType injectionType,
            Optional<ErrorType> errorType);

    void loadExchangeManager(String name, Map<String, String> properties);

    class MaterializedResultWithPlan
    {
        private final MaterializedResult materializedResult;
        private final Plan queryPlan;

        public MaterializedResultWithPlan(MaterializedResult materializedResult, Plan queryPlan)
        {
            this.materializedResult = materializedResult;
            this.queryPlan = queryPlan;
        }

        public MaterializedResult getMaterializedResult()
        {
            return materializedResult;
        }

        public Plan getQueryPlan()
        {
            return queryPlan;
        }
    }
}
