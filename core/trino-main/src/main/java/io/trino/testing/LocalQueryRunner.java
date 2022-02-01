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
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.connector.ConnectorManager;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.DefaultCatalogFactory;
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
import io.trino.cost.ComposableStatsCalculator;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculatorUsingExchanges;
import io.trino.cost.CostCalculatorWithEstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.FilterStatsCalculator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsCalculatorModule.StatsRulesProvider;
import io.trino.cost.StatsNormalizer;
import io.trino.cost.TaskCountEstimator;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.resourcegroups.NoOpResourceGroupManager;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.index.IndexManager;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.ExchangeHandleResolver;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.MetadataUtil;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.Split;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OutputFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.scalar.json.JsonExistsFunction;
import io.trino.operator.scalar.json.JsonQueryFunction;
import io.trino.operator.scalar.json.JsonValueFunction;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.GroupProviderManager;
import io.trino.server.PluginManager;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorConfig;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.ErrorType;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.GenericPartitioningSpillerFactory;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.spiller.NodeSpillConfig;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SpillerFactory;
import io.trino.spiller.SpillerStats;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.parser.SqlParser;
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
import io.trino.sql.planner.TypeAnalyzer;
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
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.type.JsonPath2016Type;
import io.trino.type.TypeDeserializer;
import io.trino.util.FinalizerService;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.connector.CatalogServiceProviderModule.createAccessControlProvider;
import static io.trino.connector.CatalogServiceProviderModule.createAnalyzePropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createColumnPropertyManager;
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
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.version.EmbedVersion.testingVersionEmbedder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class LocalQueryRunner
        implements QueryRunner
{
    private final EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig());

    private final Session defaultSession;
    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final FinalizerService finalizerService;

    private final SqlParser sqlParser;
    private final PlanFragmenter planFragmenter;
    private final InMemoryNodeManager nodeManager;
    private final BlockTypeOperators blockTypeOperators;
    private final PlannerContext plannerContext;
    private final TypeRegistry typeRegistry;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final FunctionManager functionManager;
    private final StatsCalculator statsCalculator;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TaskCountEstimator taskCountEstimator;
    private final TestingGroupProvider groupProvider;
    private final TestingAccessControlManager accessControl;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final TransactionManager transactionManager;
    private final FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private final SpillerFactory spillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;

    private final PageFunctionCompiler pageFunctionCompiler;
    private final ExpressionCompiler expressionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final JoinCompiler joinCompiler;
    private final CatalogFactory catalogFactory;
    private final ConnectorManager connectorManager;
    private final PluginManager pluginManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    private final TaskManagerConfig taskManagerConfig;
    private final boolean alwaysRevokeMemory;
    private final NodeSpillConfig nodeSpillConfig;
    private final OptimizerConfig optimizerConfig;
    private final PlanOptimizersProvider planOptimizersProvider;
    private final OperatorFactories operatorFactories;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private boolean printPlan;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static LocalQueryRunner create(Session defaultSession)
    {
        return builder(defaultSession).build();
    }

    public static Builder builder(Session defaultSession)
    {
        return new Builder(defaultSession);
    }

    // TODO clean-up constructor signature and construction of many objects https://github.com/trinodb/trino/issues/8996
    private LocalQueryRunner(
            Session defaultSession,
            FeaturesConfig featuresConfig,
            NodeSpillConfig nodeSpillConfig,
            boolean withInitialTransaction,
            boolean alwaysRevokeMemory,
            int nodeCountForStats,
            Map<String, List<PropertyMetadata<?>>> defaultSessionProperties,
            PlanOptimizersProvider planOptimizersProvider,
            MetadataProvider metadataProvider,
            OperatorFactories operatorFactories,
            Set<SystemSessionPropertiesProvider> extraSessionProperties)
    {
        requireNonNull(defaultSession, "defaultSession is null");
        requireNonNull(defaultSessionProperties, "defaultSessionProperties is null");
        checkArgument(defaultSession.getTransactionId().isEmpty() || !withInitialTransaction, "Already in transaction");

        this.taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.planOptimizersProvider = requireNonNull(planOptimizersProvider, "planOptimizersProvider is null");
        this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
        this.alwaysRevokeMemory = alwaysRevokeMemory;
        this.notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        this.yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        this.finalizerService = new FinalizerService();
        finalizerService.start();

        TypeOperators typeOperators = new TypeOperators();
        this.blockTypeOperators = new BlockTypeOperators(typeOperators);
        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig().setIncludeCoordinator(true);
        requireNonNull(featuresConfig, "featuresConfig is null");
        this.optimizerConfig = new OptimizerConfig();
        CatalogManager catalogManager = new CatalogManager();
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);

        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
        TypeManager typeManager = new InternalTypeManager(typeRegistry);
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodingManager, typeManager);

        this.globalFunctionCatalog = new GlobalFunctionCatalog();
        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(blockEncodingSerde)));
        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(featuresConfig, typeOperators, blockTypeOperators, nodeManager.getCurrentNode().getNodeVersion()));
        this.functionManager = new FunctionManager(globalFunctionCatalog);
        Metadata metadata = metadataProvider.getMetadata(
                new DisabledSystemSecurityMetadata(),
                transactionManager,
                globalFunctionCatalog,
                typeManager);
        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(
                new JsonExistsFunction(functionManager, metadata, typeManager),
                new JsonValueFunction(functionManager, metadata, typeManager),
                new JsonQueryFunction(functionManager, metadata, typeManager)));
        typeRegistry.addType(new JsonPath2016Type(new TypeDeserializer(typeManager), blockEncodingSerde));
        this.plannerContext = new PlannerContext(metadata, typeOperators, blockEncodingSerde, typeManager, functionManager);
        this.joinCompiler = new JoinCompiler(typeOperators);
        PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler, blockTypeOperators);
        this.groupProvider = new TestingGroupProvider();
        this.accessControl = new TestingAccessControlManager(transactionManager, eventListenerManager);
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());

        this.pageFunctionCompiler = new PageFunctionCompiler(functionManager, 0);
        this.expressionCompiler = new ExpressionCompiler(functionManager, pageFunctionCompiler);
        this.joinFilterFunctionCompiler = new JoinFilterFunctionCompiler(functionManager);

        HandleResolver handleResolver = new HandleResolver();

        NodeInfo nodeInfo = new NodeInfo("test");
        this.catalogFactory = new DefaultCatalogFactory(
                metadata,
                accessControl,
                handleResolver,
                nodeManager,
                pageSorter,
                pageIndexerFactory,
                nodeInfo,
                testingVersionEmbedder(),
                transactionManager,
                typeManager,
                nodeSchedulerConfig);
        this.connectorManager = new ConnectorManager(catalogFactory, catalogManager);
        this.splitManager = new SplitManager(createSplitManagerProvider(connectorManager), new QueryManagerConfig());
        this.pageSourceManager = new PageSourceManager(createPageSourceProvider(connectorManager));
        this.pageSinkManager = new PageSinkManager(createPageSinkProvider(connectorManager));
        this.indexManager = new IndexManager(createIndexProvider(connectorManager));
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, new NodeTaskMap(finalizerService)));
        this.sessionPropertyManager = createSessionPropertyManager(connectorManager, extraSessionProperties, taskManagerConfig, featuresConfig, optimizerConfig);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, blockTypeOperators, createNodePartitioningProvider(connectorManager));
        TableProceduresRegistry tableProceduresRegistry = new TableProceduresRegistry(createTableProceduresProvider(connectorManager));
        TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(createTableFunctionProvider(connectorManager));
        this.schemaPropertyManager = createSchemaPropertyManager(connectorManager);
        this.columnPropertyManager = createColumnPropertyManager(connectorManager);
        this.tablePropertyManager = createTablePropertyManager(connectorManager);
        this.materializedViewPropertyManager = createMaterializedViewPropertyManager(connectorManager);
        this.analyzePropertyManager = createAnalyzePropertyManager(connectorManager);
        TableProceduresPropertyManager tableProceduresPropertyManager = createTableProceduresPropertyManager(connectorManager);

        accessControl.setConnectorAccessControlProvider(createAccessControlProvider(connectorManager));

        this.statementAnalyzerFactory = new StatementAnalyzerFactory(
                plannerContext,
                sqlParser,
                accessControl,
                transactionManager,
                groupProvider,
                tableProceduresRegistry,
                tableFunctionRegistry,
                sessionPropertyManager,
                tablePropertyManager,
                analyzePropertyManager,
                tableProceduresPropertyManager);
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(plannerContext, statementAnalyzerFactory);
        this.statsCalculator = createNewStatsCalculator(plannerContext, typeAnalyzer);
        this.scalarStatsCalculator = new ScalarStatsCalculator(plannerContext, typeAnalyzer);
        this.taskCountEstimator = new TaskCountEstimator(() -> nodeCountForStats);
        this.costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        this.estimatedExchangesCostCalculator = new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);

        this.planFragmenter = new PlanFragmenter(metadata, functionManager, new QueryManagerConfig());

        GlobalSystemConnector globalSystemConnector = new GlobalSystemConnector(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata, accessControl),
                new TableCommentSystemTable(metadata, accessControl),
                new MaterializedViewSystemTable(metadata, accessControl),
                new SchemaPropertiesSystemTable(transactionManager, schemaPropertyManager),
                new TablePropertiesSystemTable(transactionManager, tablePropertyManager),
                new MaterializedViewPropertiesSystemTable(transactionManager, materializedViewPropertyManager),
                new ColumnPropertiesSystemTable(transactionManager, columnPropertyManager),
                new AnalyzePropertiesSystemTable(transactionManager, analyzePropertyManager),
                new TransactionsSystemTable(typeManager, transactionManager)),
                ImmutableSet.of());

        exchangeManagerRegistry = new ExchangeManagerRegistry(new ExchangeHandleResolver());
        this.pluginManager = new PluginManager(
                (loader, createClassLoader) -> {},
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
                handleResolver,
                exchangeManagerRegistry);

        connectorManager.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, GlobalSystemConnector.NAME, globalSystemConnector);

        // rewrite session to use managed SessionPropertyMetadata
        Optional<TransactionId> transactionId = withInitialTransaction ? Optional.of(transactionManager.beginTransaction(true)) : defaultSession.getTransactionId();
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                transactionId,
                defaultSession.isClientTransactionSupport(),
                defaultSession.getIdentity(),
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
                defaultSession.getProtocolHeaders());

        SpillerStats spillerStats = new SpillerStats();
        this.singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(plannerContext.getBlockEncodingSerde(), spillerStats, featuresConfig, nodeSpillConfig);
        this.partitioningSpillerFactory = new GenericPartitioningSpillerFactory(this.singleStreamSpillerFactory);
        this.spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    private static SessionPropertyManager createSessionPropertyManager(
            ConnectorServicesProvider connectorServicesProvider,
            Set<SystemSessionPropertiesProvider> extraSessionProperties,
            TaskManagerConfig taskManagerConfig,
            FeaturesConfig featuresConfig,
            OptimizerConfig optimizerConfig)
    {
        Set<SystemSessionPropertiesProvider> systemSessionProperties = ImmutableSet.<SystemSessionPropertiesProvider>builder()
                .addAll(requireNonNull(extraSessionProperties, "extraSessionProperties is null"))
                .add(new SystemSessionProperties(
                        new QueryManagerConfig(),
                        taskManagerConfig,
                        new MemoryManagerConfig(),
                        featuresConfig,
                        optimizerConfig,
                        new NodeMemoryConfig(),
                        new DynamicFilterConfig(),
                        new NodeSchedulerConfig()))
                .build();

        return CatalogServiceProviderModule.createSessionPropertyManager(systemSessionProperties, connectorServicesProvider);
    }

    private static StatsCalculator createNewStatsCalculator(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        StatsNormalizer normalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(plannerContext, typeAnalyzer);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(plannerContext, scalarStatsCalculator, normalizer);
        return new ComposableStatsCalculator(new StatsRulesProvider(plannerContext, scalarStatsCalculator, filterStatsCalculator, normalizer).get());
    }

    @Override
    public void close()
    {
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        connectorManager.stop();
        finalizerService.destroy();
        singleStreamSpillerFactory.destroy();
    }

    public void loadEventListeners()
    {
        this.eventListenerManager.loadEventListeners();
    }

    @Override
    public int getNodeCount()
    {
        return 1;
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public SqlParser getSqlParser()
    {
        return sqlParser;
    }

    public PlannerContext getPlannerContext()
    {
        return plannerContext;
    }

    @Override
    public Metadata getMetadata()
    {
        return plannerContext.getMetadata();
    }

    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    public ColumnPropertyManager getColumnPropertyManager()
    {
        return columnPropertyManager;
    }

    public MaterializedViewPropertyManager getMaterializedViewPropertyManager()
    {
        return materializedViewPropertyManager;
    }

    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        return analyzePropertyManager;
    }

    public TypeRegistry getTypeRegistry()
    {
        return typeRegistry;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return plannerContext.getTypeManager();
    }

    public GlobalFunctionCatalog getGlobalFunctionCatalog()
    {
        return globalFunctionCatalog;
    }

    @Override
    public QueryExplainer getQueryExplainer()
    {
        QueryExplainerFactory queryExplainerFactory = createQueryExplainerFactory(getPlanOptimizers(true));
        AnalyzerFactory analyzerFactory = createAnalyzerFactory(queryExplainerFactory);
        return queryExplainerFactory.createQueryExplainer(analyzerFactory);
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    @Override
    public FunctionManager getFunctionManager()
    {
        return functionManager;
    }

    public TypeOperators getTypeOperators()
    {
        return plannerContext.getTypeOperators();
    }

    public BlockTypeOperators getBlockTypeOperators()
    {
        return blockTypeOperators;
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    @Override
    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
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

    @Override
    public TestingGroupProvider getGroupProvider()
    {
        return groupProvider;
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public ExecutorService getExecutor()
    {
        return notificationExecutor;
    }

    public ScheduledExecutorService getScheduler()
    {
        return yieldExecutor;
    }

    @Override
    public Session getDefaultSession()
    {
        return defaultSession;
    }

    public ExpressionCompiler getExpressionCompiler()
    {
        return expressionCompiler;
    }

    public CatalogHandle createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        catalogFactory.addConnectorFactory(connectorFactory, ignored -> connectorFactory.getClass().getClassLoader());
        CatalogHandle catalogHandle = connectorManager.createCatalog(catalogName, connectorFactory.getName(), properties);
        nodeManager.addCurrentNodeCatalog(catalogHandle);
        return catalogHandle;
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin, ignored -> plugin.getClass().getClassLoader());
    }

    @Override
    public void addFunctions(FunctionBundle functionBundle)
    {
        globalFunctionCatalog.addFunctions(functionBundle);
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        CatalogHandle catalogHandle = connectorManager.createCatalog(catalogName, connectorName, properties);
        nodeManager.addCurrentNodeCatalog(catalogHandle);
    }

    public LocalQueryRunner printPlan()
    {
        printPlan = true;
        return this;
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        lock.readLock().lock();
        try {
            return transaction(transactionManager, accessControl)
                    .readOnly()
                    .execute(session, transactionSession -> {
                        return getMetadata().listTables(transactionSession, new QualifiedTablePrefix(catalog, schema));
                    });
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        lock.readLock().lock();
        try {
            return transaction(transactionManager, accessControl)
                    .readOnly()
                    .execute(session, transactionSession -> {
                        return MetadataUtil.tableExists(getMetadata(), transactionSession, table);
                    });
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        return executeWithPlan(session, sql, WarningCollector.NOOP).getMaterializedResult();
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector)
    {
        return inTransaction(session, transactionSession -> executeInternal(transactionSession, sql));
    }

    public <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return inTransaction(defaultSession, transactionSessionConsumer);
    }

    public <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    private MaterializedResultWithPlan executeInternal(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try (Closer closer = Closer.create()) {
            accessControl.checkCanExecuteQuery(session.getIdentity());
            AtomicReference<MaterializedResult.Builder> builder = new AtomicReference<>();
            PageConsumerOutputFactory outputFactory = new PageConsumerOutputFactory(types -> {
                builder.compareAndSet(null, MaterializedResult.resultBuilder(session, types));
                return builder.get()::page;
            });

            TaskContext taskContext = TestingTaskContext.builder(notificationExecutor, yieldExecutor, session)
                    .setMaxSpillSize(nodeSpillConfig.getMaxSpillPerNode())
                    .setQueryMaxSpillSize(nodeSpillConfig.getQueryMaxSpillPerNode())
                    .build();

            Plan plan = createPlan(session, sql, WarningCollector.NOOP);
            List<Driver> drivers = createDrivers(session, plan, outputFactory, taskContext);
            drivers.forEach(closer::register);

            boolean done = false;
            while (!done) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (alwaysRevokeMemory) {
                        driver.getDriverContext().getOperatorContexts().stream()
                                .filter(operatorContext -> operatorContext.getNestedOperatorStats().stream()
                                        .mapToLong(stats -> stats.getRevocableMemoryReservation().toBytes())
                                        .sum() > 0)
                                .forEach(OperatorContext::requestMemoryRevoking);
                    }

                    if (!driver.isFinished()) {
                        driver.processForNumberOfIterations(1);
                        processed = true;
                    }
                }
                done = !processed;
            }

            verify(builder.get() != null, "Output operator was not created");
            return new MaterializedResultWithPlan(builder.get().build(), plan);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    @Override
    public void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            InjectedFailureType injectionType,
            Optional<ErrorType> errorType)
    {
        throw new UnsupportedOperationException("failure injection is not supported");
    }

    public List<Driver> createDrivers(@Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        return createDrivers(defaultSession, sql, outputFactory, taskContext);
    }

    @Override
    public void loadExchangeManager(String name, Map<String, String> properties)
    {
        exchangeManagerRegistry.loadExchangeManager(name, properties);
    }

    public List<Driver> createDrivers(Session session, @Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        Plan plan = createPlan(session, sql, WarningCollector.NOOP);
        return createDrivers(session, plan, outputFactory, taskContext);
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode)
    {
        return planFragmenter.createSubPlans(session, plan, forceSingleNode, WarningCollector.NOOP);
    }

    private List<Driver> createDrivers(Session session, Plan plan, OutputFactory outputFactory, TaskContext taskContext)
    {
        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(
                    plan.getRoot(),
                    plan.getTypes(),
                    plannerContext.getMetadata(),
                    plannerContext.getFunctionManager(),
                    plan.getStatsAndCosts(),
                    session,
                    0,
                    false));
        }

        SubPlan subplan = createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();
        tableExecuteContextManager.registerTableExecuteContextForQuery(taskContext.getQueryContext().getQueryId());
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
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
                spillerFactory,
                singleStreamSpillerFactory,
                partitioningSpillerFactory,
                new PagesIndex.TestingFactory(false),
                joinCompiler,
                operatorFactories,
                new OrderingCompiler(plannerContext.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators,
                tableExecuteContextManager,
                exchangeManagerRegistry);

        // plan query
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getPartitioningScheme().getOutputLayout(),
                plan.getTypes(),
                subplan.getFragment().getPartitionedSources(),
                outputFactory);

        // generate splitAssignments
        List<SplitAssignment> splitAssignments = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableHandle table = tableScan.getTable();

            SplitSource splitSource = splitManager.getSplits(
                    session,
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
            boolean partitioned = partitionedSources.contains(driverFactory.getSourceId().get());
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

    @Override
    public Plan createPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        return createPlan(session, sql, OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, WarningCollector warningCollector)
    {
        return createPlan(session, sql, stage, true, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, WarningCollector warningCollector)
    {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        return createPlan(session, sql, getPlanOptimizers(forceSingleNode), stage, warningCollector);
    }

    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        return planOptimizersProvider.getPlanOptimizers(
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
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
                new RuleStatsRecorder()).get();
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, WarningCollector warningCollector)
    {
        return createPlan(session, sql, optimizers, OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector)
    {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        AnalyzerFactory analyzerFactory = createAnalyzerFactory(createQueryExplainerFactory(optimizers));
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                session,
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                optimizers,
                new PlanSanityChecker(true),
                idAllocator,
                getPlannerContext(),
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                statsCalculator,
                costCalculator,
                warningCollector);

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());
        // make LocalQueryRunner always compute plan statistics for test purposes
        return logicalPlanner.plan(analysis, stage);
    }

    private QueryExplainerFactory createQueryExplainerFactory(List<PlanOptimizer> optimizers)
    {
        return new QueryExplainerFactory(
                () -> optimizers,
                planFragmenter,
                plannerContext,
                statementAnalyzerFactory,
                statsCalculator,
                costCalculator);
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
                                materializedViewPropertyManager),
                        new ShowStatsRewrite(plannerContext.getMetadata(), queryExplainerFactory, statsCalculator),
                        new ExplainRewrite(queryExplainerFactory, new QueryPreparer(sqlParser)))));
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(1000)).getSplits();
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    public interface PlanOptimizersProvider
    {
        PlanOptimizersFactory getPlanOptimizers(
                PlannerContext plannerContext,
                TypeAnalyzer typeAnalyzer,
                TaskManagerConfig taskManagerConfig,
                boolean forceSingleNode,
                SplitManager splitManager,
                PageSourceManager pageSourceManager,
                StatsCalculator statsCalculator,
                ScalarStatsCalculator scalarStatsCalculator,
                CostCalculator costCalculator,
                CostCalculator estimatedExchangesCostCalculator,
                CostComparator costComparator,
                TaskCountEstimator taskCountEstimator,
                NodePartitioningManager nodePartitioningManager,
                RuleStatsRecorder ruleStats);
    }

    public interface MetadataProvider
    {
        Metadata getMetadata(
                SystemSecurityMetadata systemSecurityMetadata,
                TransactionManager transactionManager,
                GlobalFunctionCatalog globalFunctionCatalog,
                TypeManager typeManager);
    }

    public static class Builder
    {
        private final Session defaultSession;
        private FeaturesConfig featuresConfig = new FeaturesConfig();
        private NodeSpillConfig nodeSpillConfig = new NodeSpillConfig();
        private boolean initialTransaction;
        private boolean alwaysRevokeMemory;
        private Map<String, List<PropertyMetadata<?>>> defaultSessionProperties = ImmutableMap.of();
        private Set<SystemSessionPropertiesProvider> extraSessionProperties = ImmutableSet.of();
        private int nodeCountForStats;
        private PlanOptimizersProvider planOptimizersProvider = PlanOptimizers::new;
        private MetadataProvider metadataProvider = MetadataManager::new;
        private OperatorFactories operatorFactories = new TrinoOperatorFactories();

        private Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public Builder withFeaturesConfig(FeaturesConfig featuresConfig)
        {
            this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
            return this;
        }

        public Builder withNodeSpillConfig(NodeSpillConfig nodeSpillConfig)
        {
            this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
            return this;
        }

        public Builder withInitialTransaction()
        {
            this.initialTransaction = true;
            return this;
        }

        public Builder withAlwaysRevokeMemory()
        {
            this.alwaysRevokeMemory = true;
            return this;
        }

        public Builder withDefaultSessionProperties(Map<String, List<PropertyMetadata<?>>> defaultSessionProperties)
        {
            this.defaultSessionProperties = requireNonNull(defaultSessionProperties, "defaultSessionProperties is null");
            return this;
        }

        public Builder withNodeCountForStats(int nodeCountForStats)
        {
            this.nodeCountForStats = nodeCountForStats;
            return this;
        }

        public Builder withPlanOptimizersProvider(PlanOptimizersProvider planOptimizersProvider)
        {
            this.planOptimizersProvider = requireNonNull(planOptimizersProvider, "planOptimizersProvider is null");
            return this;
        }

        public Builder withMetadataProvider(MetadataProvider metadataProvider)
        {
            this.metadataProvider = metadataProvider;
            return this;
        }

        public Builder withOperatorFactories(OperatorFactories operatorFactories)
        {
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
            return this;
        }

        /**
         * This method is required to pass in system session properties and their
         * metadata for Trino extension modules (separate from the default system
         * session properties, provided to the query runner at {@link LocalQueryRunner#createSessionPropertyManager}.
         */
        public Builder withExtraSystemSessionProperties(Set<SystemSessionPropertiesProvider> extraSessionProperties)
        {
            this.extraSessionProperties = ImmutableSet.copyOf(requireNonNull(extraSessionProperties, "extraSessionProperties is null"));
            return this;
        }

        public LocalQueryRunner build()
        {
            return new LocalQueryRunner(
                    defaultSession,
                    featuresConfig,
                    nodeSpillConfig,
                    initialTransaction,
                    alwaysRevokeMemory,
                    nodeCountForStats,
                    defaultSessionProperties,
                    planOptimizersProvider,
                    metadataProvider,
                    operatorFactories,
                    extraSessionProperties);
        }
    }
}
