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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.connector.ConnectorName;
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
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
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
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.InternalNodeManager;
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
import io.trino.operator.OutputFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.operator.TaskContext;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.scalar.json.JsonExistsFunction;
import io.trino.operator.scalar.json.JsonQueryFunction;
import io.trino.operator.scalar.json.JsonValueFunction;
import io.trino.operator.table.ExcludeColumns.ExcludeColumnsFunction;
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
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.exchange.ExchangeManager;
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
import io.trino.sql.analyzer.SessionTimeProvider;
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
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.OutputNode;
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
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
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
    private final InternalNodeManager nodeManager;
    private final TypeOperators typeOperators;
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
    private final TestingGroupProviderManager groupProvider;
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
    private final CoordinatorDynamicCatalogManager catalogManager;
    private final PluginManager pluginManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    private final TaskManagerConfig taskManagerConfig;
    private final boolean alwaysRevokeMemory;
    private final DataSize maxSpillPerNode;
    private final DataSize queryMaxSpillPerNode;
    private final OptimizerConfig optimizerConfig;
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
            MetadataProvider metadataProvider,
            Set<SystemSessionPropertiesProvider> extraSessionProperties)
    {
        requireNonNull(defaultSession, "defaultSession is null");
        requireNonNull(defaultSessionProperties, "defaultSessionProperties is null");
        checkArgument(defaultSession.getTransactionId().isEmpty() || !withInitialTransaction, "Already in transaction");

        Tracer tracer = noopTracer();
        this.taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.maxSpillPerNode = nodeSpillConfig.getMaxSpillPerNode();
        this.queryMaxSpillPerNode = nodeSpillConfig.getQueryMaxSpillPerNode();
        this.alwaysRevokeMemory = alwaysRevokeMemory;
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
        requireNonNull(featuresConfig, "featuresConfig is null");
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
        typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
        TypeManager typeManager = new InternalTypeManager(typeRegistry);
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodingManager, typeManager);

        this.globalFunctionCatalog = new GlobalFunctionCatalog();
        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(blockEncodingSerde)));
        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(featuresConfig, typeOperators, blockTypeOperators, nodeManager.getCurrentNode().getNodeVersion()));
        Metadata metadata = metadataProvider.getMetadata(
                new DisabledSystemSecurityMetadata(),
                transactionManager,
                globalFunctionCatalog,
                typeManager);
        typeRegistry.addType(new JsonPath2016Type(new TypeDeserializer(typeManager), blockEncodingSerde));
        this.joinCompiler = new JoinCompiler(typeOperators);
        PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler, typeOperators);
        this.groupProvider = new TestingGroupProviderManager();
        this.accessControl = new TestingAccessControlManager(transactionManager, eventListenerManager);
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());

        HandleResolver handleResolver = new HandleResolver();

        NodeInfo nodeInfo = new NodeInfo("test");
        catalogFactory.setCatalogFactory(new DefaultCatalogFactory(
                metadata,
                accessControl,
                handleResolver,
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
        this.sessionPropertyManager = createSessionPropertyManager(catalogManager, extraSessionProperties, taskManagerConfig, featuresConfig, optimizerConfig);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, typeOperators, createNodePartitioningProvider(catalogManager));
        TableProceduresRegistry tableProceduresRegistry = new TableProceduresRegistry(createTableProceduresProvider(catalogManager));
        this.functionManager = new FunctionManager(createFunctionProvider(catalogManager), globalFunctionCatalog);
        TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(createTableFunctionProvider(catalogManager));
        this.schemaPropertyManager = createSchemaPropertyManager(catalogManager);
        this.columnPropertyManager = createColumnPropertyManager(catalogManager);
        this.tablePropertyManager = createTablePropertyManager(catalogManager);
        this.materializedViewPropertyManager = createMaterializedViewPropertyManager(catalogManager);
        this.analyzePropertyManager = createAnalyzePropertyManager(catalogManager);
        TableProceduresPropertyManager tableProceduresPropertyManager = createTableProceduresPropertyManager(catalogManager);

        accessControl.setConnectorAccessControlProvider(createAccessControlProvider(catalogManager));

        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(
                new JsonExistsFunction(functionManager, metadata, typeManager),
                new JsonValueFunction(functionManager, metadata, typeManager),
                new JsonQueryFunction(functionManager, metadata, typeManager)));

        this.plannerContext = new PlannerContext(metadata, typeOperators, blockEncodingSerde, typeManager, functionManager, tracer);
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

        this.planFragmenter = new PlanFragmenter(metadata, functionManager, transactionManager, catalogManager, new QueryManagerConfig());

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

        exchangeManagerRegistry = new ExchangeManagerRegistry();
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

        catalogManager.registerGlobalSystemConnector(globalSystemConnector);

        // rewrite session to use managed SessionPropertyMetadata
        Optional<TransactionId> transactionId = withInitialTransaction ? Optional.of(transactionManager.beginTransaction(true)) : defaultSession.getTransactionId();
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                Span.getInvalid(),
                transactionId,
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
        catalogManager.stop();
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
    public ExchangeManager getExchangeManager()
    {
        return exchangeManagerRegistry.getExchangeManager();
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

    public TaskCountEstimator getTaskCountEstimator()
    {
        return taskCountEstimator;
    }

    @Override
    public TestingGroupProviderManager getGroupProvider()
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

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        catalogFactory.addConnectorFactory(connectorFactory, ignored -> connectorFactory.getClass().getClassLoader());
        catalogManager.createCatalog(catalogName, new ConnectorName(connectorFactory.getName()), properties, false);
    }

    public void registerCatalogFactory(ConnectorFactory connectorFactory)
    {
        catalogFactory.addConnectorFactory(connectorFactory, ignored -> connectorFactory.getClass().getClassLoader());
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
        catalogManager.createCatalog(catalogName, new ConnectorName(connectorName), properties, false);
    }

    public CatalogManager getCatalogManager()
    {
        return catalogManager;
    }

    public LocalQueryRunner printPlan()
    {
        printPlan = true;
        return this;
    }

    public CatalogHandle getCatalogHandle(String catalogName)
    {
        return inTransaction(transactionSession -> getMetadata().getCatalogHandle(transactionSession, catalogName)).orElseThrow();
    }

    public TableHandle getTableHandle(String catalogName, String schemaName, String tableName)
    {
        return inTransaction(transactionSession ->
                getMetadata().getTableHandle(
                        transactionSession,
                        new QualifiedObjectName(catalogName, schemaName, tableName))
                .orElseThrow());
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
                    .setMaxSpillSize(maxSpillPerNode)
                    .setQueryMaxSpillSize(queryMaxSpillPerNode)
                    .build();

            Plan plan = createPlan(session, sql, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
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
            builder.get().columnNames(((OutputNode) plan.getRoot()).getColumnNames());
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
        Plan plan = createPlan(session, sql, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
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
                new OrderingCompiler(plannerContext.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators,
                typeOperators,
                tableExecuteContextManager,
                exchangeManagerRegistry,
                nodeManager.getCurrentNode().getNodeVersion());

        // plan query
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getOutputPartitioningScheme().getOutputLayout(),
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
    public Plan createPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        return createPlan(session, sql, OPTIMIZED_AND_VALIDATED, warningCollector, planOptimizersStatsCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        return createPlan(session, sql, stage, true, warningCollector, planOptimizersStatsCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        return createPlan(session, sql, getPlanOptimizers(forceSingleNode), stage, warningCollector, planOptimizersStatsCollector);
    }

    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        return new PlanOptimizers(
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

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        return createPlan(session, sql, optimizers, OPTIMIZED_AND_VALIDATED, warningCollector, planOptimizersStatsCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

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
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                statsCalculator,
                costCalculator,
                warningCollector,
                planOptimizersStatsCollector);

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
                costCalculator,
                new NodeVersion("test"));
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
                .findAll();
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
        private MetadataProvider metadataProvider = MetadataManager::new;

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

        public Builder withMetadataProvider(MetadataProvider metadataProvider)
        {
            this.metadataProvider = metadataProvider;
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
                    metadataProvider,
                    extraSessionProperties);
        }
    }
}
