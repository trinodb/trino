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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.InMemoryCatalogStore;
import io.trino.connector.LazyCatalogFactory;
import io.trino.connector.system.AnalyzePropertiesSystemTable;
import io.trino.connector.system.CatalogSystemTable;
import io.trino.connector.system.ColumnPropertiesSystemTable;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.connector.system.MaterializedViewPropertiesSystemTable;
import io.trino.connector.system.MaterializedViewSystemTable;
import io.trino.connector.system.NodeSystemTable;
import io.trino.connector.system.SchemaPropertiesSystemTable;
import io.trino.connector.system.TableCommentSystemTable;
import io.trino.connector.system.TablePropertiesSystemTable;
import io.trino.connector.system.TransactionsSystemTable;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.ComposableStatsCalculator;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculatorUsingExchanges;
import io.trino.cost.CostCalculatorWithEstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.FilterStatsCalculator;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsCalculatorModule.StatsRulesProvider;
import io.trino.cost.StatsNormalizer;
import io.trino.cost.TaskCountEstimator;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.resourcegroups.NoOpResourceGroupManager;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.Split;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.TypeRegistry;
import io.trino.metadata.ViewPropertyManager;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.operator.TaskContext;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.index.IndexManager;
import io.trino.operator.scalar.json.JsonExistsFunction;
import io.trino.operator.scalar.json.JsonQueryFunction;
import io.trino.operator.scalar.json.JsonValueFunction;
import io.trino.operator.table.ExcludeColumnsFunction;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.GroupProviderManager;
import io.trino.server.PluginManager;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorConfig;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.Plugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.analyzer.SessionTimeProvider;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.AdaptivePlanner;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizers;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.optimizations.AdaptivePlanOptimizer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.rewrite.DescribeInputRewrite;
import io.trino.sql.rewrite.DescribeOutputRewrite;
import io.trino.sql.rewrite.ExplainRewrite;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.ShowStatsRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.testing.NullOutputOperator.NullOutputFactory;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.type.JsonPath2016Type;
import io.trino.type.TypeDeserializer;
import io.trino.util.FinalizerService;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogServiceProviderModule.createAccessControlProvider;
import static io.trino.connector.CatalogServiceProviderModule.createAnalyzePropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createColumnPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createFunctionProvider;
import static io.trino.connector.CatalogServiceProviderModule.createIndexProvider;
import static io.trino.connector.CatalogServiceProviderModule.createMaterializedViewPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createNodePartitioningProvider;
import static io.trino.connector.CatalogServiceProviderModule.createPageSinkProvider;
import static io.trino.connector.CatalogServiceProviderModule.createPageSourceProvider;
import static io.trino.connector.CatalogServiceProviderModule.createSchemaPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createSplitManagerProvider;
import static io.trino.connector.CatalogServiceProviderModule.createTableFunctionProvider;
import static io.trino.connector.CatalogServiceProviderModule.createTableProceduresPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createTableProceduresProvider;
import static io.trino.connector.CatalogServiceProviderModule.createTablePropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createViewPropertyManager;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.spiller.SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.util.EmbedVersion.testingVersionEmbedder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class PlanTester
        implements Closeable
{
    private final Session defaultSession;
    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final FinalizerService finalizerService;

    private final SqlParser sqlParser;
    private final PlanFragmenter planFragmenter;
    private final InternalNodeManager nodeManager;
    private final TypeOperators typeOperators;
    private final BlockTypeOperators blockTypeOperators;
    private final PlannerContext plannerContext;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final StatsCalculator statsCalculator;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TaskCountEstimator taskCountEstimator;
    private final TestingAccessControlManager accessControl;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final TransactionManager transactionManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final ViewPropertyManager viewPropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;

    private final PageFunctionCompiler pageFunctionCompiler;
    private final ExpressionCompiler expressionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final JoinCompiler joinCompiler;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final CatalogFactory catalogFactory;
    private final CoordinatorDynamicCatalogManager catalogManager;
    private final PluginManager pluginManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    private final TaskManagerConfig taskManagerConfig;
    private final OptimizerConfig optimizerConfig;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private boolean printPlan;

    public static PlanTester create(Session defaultSession)
    {
        return new PlanTester(defaultSession, 1);
    }

    public static PlanTester create(Session defaultSession, int nodeCountForStats)
    {
        return new PlanTester(defaultSession, nodeCountForStats);
    }

    private PlanTester(Session defaultSession, int nodeCountForStats)
    {
        requireNonNull(defaultSession, "defaultSession is null");

        Tracer tracer = noopTracer();
        this.taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        this.notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        this.yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        this.finalizerService = new FinalizerService();
        finalizerService.start();

        this.typeOperators = new TypeOperators();
        this.blockTypeOperators = new BlockTypeOperators(typeOperators);
        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig().setIncludeCoordinator(true);
        this.optimizerConfig = new OptimizerConfig();
        LazyCatalogFactory catalogFactory = new LazyCatalogFactory();
        this.catalogFactory = catalogFactory;
        this.catalogManager = new CoordinatorDynamicCatalogManager(new InMemoryCatalogStore(), catalogFactory, directExecutor());
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);

        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        TypeRegistry typeRegistry = new TypeRegistry(typeOperators, new FeaturesConfig());
        TypeManager typeManager = new InternalTypeManager(typeRegistry);
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodingManager, typeManager);

        this.globalFunctionCatalog = new GlobalFunctionCatalog(
                () -> getPlannerContext().getMetadata(),
                () -> getPlannerContext().getTypeManager(),
                () -> getPlannerContext().getFunctionManager());
        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, blockTypeOperators, nodeManager.getCurrentNode().getNodeVersion()));
        TestingGroupProviderManager groupProvider = new TestingGroupProviderManager();
        LanguageFunctionManager languageFunctionManager = new LanguageFunctionManager(sqlParser, typeManager, groupProvider, blockEncodingSerde);
        Metadata metadata = new MetadataManager(
                new DisabledSystemSecurityMetadata(),
                transactionManager,
                globalFunctionCatalog,
                languageFunctionManager,
                typeManager);
        typeRegistry.addType(new JsonPath2016Type(new TypeDeserializer(typeManager), blockEncodingSerde));
        this.joinCompiler = new JoinCompiler(typeOperators);
        this.hashStrategyCompiler = new FlatHashStrategyCompiler(typeOperators);
        PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(hashStrategyCompiler);
        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig());
        this.accessControl = new TestingAccessControlManager(transactionManager, eventListenerManager);
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());

        NodeInfo nodeInfo = new NodeInfo("test");
        catalogFactory.setCatalogFactory(new DefaultCatalogFactory(
                metadata,
                accessControl,
                nodeManager,
                pageSorter,
                pageIndexerFactory,
                nodeInfo,
                testingVersionEmbedder(),
                OpenTelemetry.noop(),
                transactionManager,
                typeManager,
                nodeSchedulerConfig,
                optimizerConfig));
        this.splitManager = new SplitManager(createSplitManagerProvider(catalogManager), tracer, new QueryManagerConfig());
        this.pageSourceManager = new PageSourceManager(createPageSourceProvider(catalogManager));
        this.pageSinkManager = new PageSinkManager(createPageSinkProvider(catalogManager));
        this.indexManager = new IndexManager(createIndexProvider(catalogManager));
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, new NodeTaskMap(finalizerService)));
        this.sessionPropertyManager = createSessionPropertyManager(catalogManager, taskManagerConfig, optimizerConfig);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, typeOperators, createNodePartitioningProvider(catalogManager));
        TableProceduresRegistry tableProceduresRegistry = new TableProceduresRegistry(createTableProceduresProvider(catalogManager));
        FunctionManager functionManager = new FunctionManager(createFunctionProvider(catalogManager), globalFunctionCatalog, languageFunctionManager);
        TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(createTableFunctionProvider(catalogManager));
        this.schemaPropertyManager = createSchemaPropertyManager(catalogManager);
        this.columnPropertyManager = createColumnPropertyManager(catalogManager);
        this.tablePropertyManager = createTablePropertyManager(catalogManager);
        this.viewPropertyManager = createViewPropertyManager(catalogManager);
        this.materializedViewPropertyManager = createMaterializedViewPropertyManager(catalogManager);
        this.analyzePropertyManager = createAnalyzePropertyManager(catalogManager);
        TableProceduresPropertyManager tableProceduresPropertyManager = createTableProceduresPropertyManager(catalogManager);

        accessControl.setConnectorAccessControlProvider(createAccessControlProvider(catalogManager));

        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(
                new JsonExistsFunction(functionManager, metadata, typeManager),
                new JsonValueFunction(functionManager, metadata, typeManager),
                new JsonQueryFunction(functionManager, metadata, typeManager)));

        this.plannerContext = new PlannerContext(metadata, typeOperators, blockEncodingSerde, typeManager, functionManager, languageFunctionManager, tracer);
        this.pageFunctionCompiler = new PageFunctionCompiler(functionManager, 0);
        this.expressionCompiler = new ExpressionCompiler(functionManager, pageFunctionCompiler);
        this.joinFilterFunctionCompiler = new JoinFilterFunctionCompiler(functionManager);

        this.statementAnalyzerFactory = new StatementAnalyzerFactory(
                plannerContext,
                sqlParser,
                SessionTimeProvider.DEFAULT,
                accessControl,
                transactionManager,
                groupProvider,
                tableProceduresRegistry,
                tableFunctionRegistry,
                tablePropertyManager,
                analyzePropertyManager,
                tableProceduresPropertyManager);
        this.statsCalculator = createNewStatsCalculator(plannerContext);
        this.scalarStatsCalculator = new ScalarStatsCalculator(plannerContext);
        this.taskCountEstimator = new TaskCountEstimator(() -> nodeCountForStats);
        this.costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        this.estimatedExchangesCostCalculator = new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);

        this.planFragmenter = new PlanFragmenter(metadata, functionManager, transactionManager, catalogManager, languageFunctionManager, new QueryManagerConfig());

        GlobalSystemConnector globalSystemConnector = new GlobalSystemConnector(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata, accessControl),
                new TableCommentSystemTable(metadata, accessControl),
                new MaterializedViewSystemTable(metadata, accessControl),
                new SchemaPropertiesSystemTable(metadata, accessControl, schemaPropertyManager),
                new TablePropertiesSystemTable(metadata, accessControl, tablePropertyManager),
                new MaterializedViewPropertiesSystemTable(metadata, accessControl, materializedViewPropertyManager),
                new ColumnPropertiesSystemTable(metadata, accessControl, columnPropertyManager),
                new AnalyzePropertiesSystemTable(metadata, accessControl, analyzePropertyManager),
                new TransactionsSystemTable(typeManager, transactionManager)),
                ImmutableSet.of(),
                ImmutableSet.of(new ExcludeColumnsFunction()));

        exchangeManagerRegistry = new ExchangeManagerRegistry(OpenTelemetry.noop(), noopTracer());
        this.pluginManager = new PluginManager(
                (loader, createClassLoader) -> {},
                Optional.empty(),
                catalogFactory,
                globalFunctionCatalog,
                new NoOpResourceGroupManager(),
                accessControl,
                Optional.of(new PasswordAuthenticatorManager(new PasswordAuthenticatorConfig())),
                new CertificateAuthenticatorManager(),
                Optional.of(new HeaderAuthenticatorManager(new HeaderAuthenticatorConfig())),
                eventListenerManager,
                new GroupProviderManager(),
                new SessionPropertyDefaults(nodeInfo, accessControl),
                typeRegistry,
                blockEncodingManager,
                new HandleResolver(),
                exchangeManagerRegistry);

        catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        languageFunctionManager.setPlannerContext(plannerContext);

        // rewrite session to use managed SessionPropertyMetadata
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                Span.getInvalid(),
                defaultSession.getTransactionId(),
                defaultSession.isClientTransactionSupport(),
                defaultSession.getIdentity(),
                defaultSession.getOriginalIdentity(),
                defaultSession.getSource(),
                defaultSession.getCatalog(),
                defaultSession.getSchema(),
                defaultSession.getPath(),
                defaultSession.getTraceToken(),
                defaultSession.getTimeZoneKey(),
                defaultSession.getLocale(),
                defaultSession.getRemoteUserAddress(),
                defaultSession.getUserAgent(),
                defaultSession.getClientInfo(),
                defaultSession.getClientTags(),
                defaultSession.getClientCapabilities(),
                defaultSession.getResourceEstimates(),
                defaultSession.getStart(),
                defaultSession.getSystemProperties(),
                defaultSession.getCatalogProperties(),
                sessionPropertyManager,
                defaultSession.getPreparedStatements(),
                defaultSession.getProtocolHeaders(),
                defaultSession.getExchangeEncryptionKey());
    }

    private static SessionPropertyManager createSessionPropertyManager(
            ConnectorServicesProvider connectorServicesProvider,
            TaskManagerConfig taskManagerConfig,
            OptimizerConfig optimizerConfig)
    {
        SystemSessionProperties sessionProperties = new SystemSessionProperties(
                new QueryManagerConfig(),
                taskManagerConfig,
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                optimizerConfig,
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig());
        return CatalogServiceProviderModule.createSessionPropertyManager(ImmutableSet.of(sessionProperties), connectorServicesProvider);
    }

    private static StatsCalculator createNewStatsCalculator(PlannerContext plannerContext)
    {
        StatsNormalizer normalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(plannerContext);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(plannerContext, scalarStatsCalculator, normalizer);
        return new ComposableStatsCalculator(new StatsRulesProvider(scalarStatsCalculator, filterStatsCalculator, normalizer).get());
    }

    @Override
    public void close()
    {
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        catalogManager.stop();
        finalizerService.destroy();
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public PlannerContext getPlannerContext()
    {
        return plannerContext;
    }

    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        return analyzePropertyManager;
    }

    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    public StatsCalculator getStatsCalculator()
    {
        return statsCalculator;
    }

    public CostCalculator getCostCalculator()
    {
        return costCalculator;
    }

    public CostCalculator getEstimatedExchangesCostCalculator()
    {
        return estimatedExchangesCostCalculator;
    }

    public TaskCountEstimator getTaskCountEstimator()
    {
        return taskCountEstimator;
    }

    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public Session getDefaultSession()
    {
        return defaultSession;
    }

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        catalogFactory.addConnectorFactory(connectorFactory);
        catalogManager.createCatalog(new CatalogName(catalogName), new ConnectorName(connectorFactory.getName()), properties, false);
    }

    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    public void addFunctions(FunctionBundle functionBundle)
    {
        globalFunctionCatalog.addFunctions(functionBundle);
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        catalogManager.createCatalog(new CatalogName(catalogName), new ConnectorName(connectorName), properties, false);
    }

    public CatalogManager getCatalogManager()
    {
        return catalogManager;
    }

    public Connector getConnector(String catalogName)
    {
        return catalogManager
                .getConnectorServices(getCatalogHandle(catalogName))
                .getConnector();
    }

    public PlanTester printPlan()
    {
        printPlan = true;
        return this;
    }

    public <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return inTransaction(getDefaultSession(), transactionSessionConsumer);
    }

    public <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(getTransactionManager(), getPlannerContext().getMetadata(), getAccessControl())
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    public CatalogHandle getCatalogHandle(String catalogName)
    {
        return inTransaction(transactionSession -> getPlannerContext().getMetadata().getCatalogHandle(transactionSession, catalogName)).orElseThrow();
    }

    public TableHandle getTableHandle(String catalogName, String schemaName, String tableName)
    {
        return inTransaction(transactionSession ->
                getPlannerContext().getMetadata().getTableHandle(
                                transactionSession,
                                new QualifiedObjectName(catalogName, schemaName, tableName))
                        .orElseThrow());
    }

    public void executeStatement(@Language("SQL") String sql)
    {
        accessControl.checkCanExecuteQuery(defaultSession.getIdentity(), defaultSession.getQueryId());

        inTransaction(defaultSession, transactionSession -> {
            try (Closer closer = Closer.create()) {
                List<Driver> drivers = createDrivers(transactionSession, sql);
                drivers.forEach(closer::register);

                boolean done = false;
                while (!done) {
                    boolean processed = false;
                    for (Driver driver : drivers) {
                        if (!driver.isFinished()) {
                            driver.processForNumberOfIterations(1);
                            processed = true;
                        }
                    }
                    done = !processed;
                }
                return null;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode)
    {
        plannerContext.getLanguageFunctionManager().tryRegisterQuery(session);
        return planFragmenter.createSubPlans(session, plan, forceSingleNode, NOOP);
    }

    private List<Driver> createDrivers(Session session, @Language("SQL") String sql)
    {
        Plan plan = createPlan(session, sql);
        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(
                    plan.getRoot(),
                    plannerContext.getMetadata(),
                    plannerContext.getFunctionManager(),
                    plan.getStatsAndCosts(),
                    session,
                    0,
                    false));
        }

        SubPlan subplan = createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected sub-plan to have no children");
        }

        TaskContext taskContext = createTaskContext(notificationExecutor, yieldExecutor, session);
        TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();
        tableExecuteContextManager.registerTableExecuteContextForQuery(taskContext.getQueryContext().getQueryId());
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                plannerContext,
                Optional.empty(),
                pageSourceManager,
                indexManager,
                nodePartitioningManager,
                pageSinkManager,
                null,
                expressionCompiler,
                pageFunctionCompiler,
                joinFilterFunctionCompiler,
                new IndexJoinLookupStats(),
                this.taskManagerConfig,
                new GenericSpillerFactory(unsupportedSingleStreamSpillerFactory()),
                unsupportedSingleStreamSpillerFactory(),
                unsupportedPartitioningSpillerFactory(),
                new PagesIndex.TestingFactory(false),
                joinCompiler,
                hashStrategyCompiler,
                new OrderingCompiler(plannerContext.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators,
                typeOperators,
                tableExecuteContextManager,
                exchangeManagerRegistry,
                nodeManager.getCurrentNode().getNodeVersion(),
                new CompilerConfig());

        // plan query
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getOutputPartitioningScheme().getOutputLayout(),
                subplan.getFragment().getPartitionedSources(),
                new NullOutputFactory());

        // generate splitAssignments
        List<SplitAssignment> splitAssignments = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableHandle table = tableScan.getTable();

            SplitSource splitSource = splitManager.getSplits(
                    session,
                    Span.getInvalid(),
                    table,
                    EMPTY,
                    alwaysTrue());

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getNextBatch(splitSource)) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }

            splitAssignments.add(new SplitAssignment(tableScan.getId(), scheduledSplits.build(), true));
        }

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, DriverFactory> driverFactoriesBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                if (driverFactory.getSourceId().isPresent()) {
                    checkState(driverFactoriesBySource.put(driverFactory.getSourceId().get(), driverFactory) == null);
                }
                else {
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false).addDriverContext();
                    Driver driver = driverFactory.createDriver(driverContext);
                    drivers.add(driver);
                }
            }
        }

        // add split assignments to the drivers
        ImmutableSet<PlanNodeId> partitionedSources = ImmutableSet.copyOf(subplan.getFragment().getPartitionedSources());
        for (SplitAssignment splitAssignment : splitAssignments) {
            DriverFactory driverFactory = driverFactoriesBySource.get(splitAssignment.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = partitionedSources.contains(driverFactory.getSourceId().orElseThrow());
            for (ScheduledSplit split : splitAssignment.getSplits()) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSplitAssignment(new SplitAssignment(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql)
    {
        return createPlan(session, sql, getPlanOptimizers(true), OPTIMIZED_AND_VALIDATED, NOOP, createPlanOptimizersStatsCollector());
    }

    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        return getPlanOptimizersFactory(forceSingleNode).getPlanOptimizers();
    }

    public List<AdaptivePlanOptimizer> getAdaptivePlanOptimizers()
    {
        return getPlanOptimizersFactory(false).getAdaptivePlanOptimizers();
    }

    public PlanOptimizersFactory getPlanOptimizersFactory(boolean forceSingleNode)
    {
        return new PlanOptimizers(
                plannerContext,
                taskManagerConfig,
                forceSingleNode,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(optimizerConfig),
                taskCountEstimator,
                nodePartitioningManager,
                new RuleStatsRecorder());
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        // session must be in a transaction registered with the transaction manager in this query runner
        transactionManager.getTransactionInfo(session.getRequiredTransactionId());

        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, preparedQuery.getStatement());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        AnalyzerFactory analyzerFactory = createAnalyzerFactory(createQueryExplainerFactory(optimizers));
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                session,
                preparedQuery.getParameters(),
                bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector,
                planOptimizersStatsCollector);

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                optimizers,
                new PlanSanityChecker(true),
                idAllocator,
                getPlannerContext(),
                statsCalculator,
                costCalculator,
                warningCollector,
                planOptimizersStatsCollector,
                new CachingTableStatsProvider(getPlannerContext().getMetadata(), session));

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());
        // make PlanTester always compute plan statistics for test purposes
        return logicalPlanner.plan(analysis, stage);
    }

    public SubPlan createAdaptivePlan(Session session, SubPlan subPlan, List<AdaptivePlanOptimizer> optimizers, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector, RuntimeInfoProvider runtimeInfoProvider)
    {
        AdaptivePlanner adaptivePlanner = new AdaptivePlanner(
                session,
                getPlannerContext(),
                optimizers,
                planFragmenter,
                new PlanSanityChecker(false),
                warningCollector,
                planOptimizersStatsCollector,
                new CachingTableStatsProvider(getPlannerContext().getMetadata(), session));
        return adaptivePlanner.optimize(subPlan, runtimeInfoProvider);
    }

    private QueryExplainerFactory createQueryExplainerFactory(List<PlanOptimizer> optimizers)
    {
        return new QueryExplainerFactory(
                createPlanOptimizersFactory(optimizers),
                planFragmenter,
                plannerContext,
                statsCalculator,
                costCalculator,
                new NodeVersion("test"));
    }

    private PlanOptimizersFactory createPlanOptimizersFactory(List<PlanOptimizer> optimizers)
    {
        return new PlanOptimizersFactory()
        {
            @Override
            public List<PlanOptimizer> getPlanOptimizers()
            {
                return optimizers;
            }

            @Override
            public List<AdaptivePlanOptimizer> getAdaptivePlanOptimizers()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private AnalyzerFactory createAnalyzerFactory(QueryExplainerFactory queryExplainerFactory)
    {
        return new AnalyzerFactory(
                statementAnalyzerFactory,
                new StatementRewrite(ImmutableSet.of(
                        new DescribeInputRewrite(sqlParser),
                        new DescribeOutputRewrite(sqlParser),
                        new ShowQueriesRewrite(
                                plannerContext.getMetadata(),
                                sqlParser,
                                accessControl,
                                sessionPropertyManager,
                                schemaPropertyManager,
                                columnPropertyManager,
                                tablePropertyManager,
                                viewPropertyManager,
                                materializedViewPropertyManager),
                        new ShowStatsRewrite(plannerContext.getMetadata(), queryExplainerFactory, statsCalculator),
                        new ExplainRewrite(queryExplainerFactory, new QueryPreparer(sqlParser)))),
                plannerContext.getTracer());
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(1000)).getSplits();
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll().stream()
                .map(TableScanNode.class::cast)
                .collect(toImmutableList());
    }
}
