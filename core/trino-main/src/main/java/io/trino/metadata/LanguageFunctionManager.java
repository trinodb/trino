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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.TaskId;
import io.trino.execution.warnings.WarningCollector;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.security.AccessControl;
import io.trino.security.ViewAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.LanguageFunctionEngine;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.SqlPath;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.routine.SqlRoutineAnalysis;
import io.trino.sql.routine.SqlRoutineAnalyzer;
import io.trino.sql.routine.SqlRoutineCompiler;
import io.trino.sql.routine.SqlRoutineHash;
import io.trino.sql.routine.SqlRoutinePlanner;
import io.trino.sql.routine.ir.IrRoutine;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.ParameterDeclaration;
import io.trino.sql.tree.Property;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.PropertyUtil.evaluateProperties;
import static io.trino.metadata.PropertyUtil.toSqlProperties;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.routine.SqlRoutineAnalyzer.extractFunctionMetadata;
import static io.trino.sql.routine.SqlRoutineAnalyzer.getLanguage;
import static io.trino.sql.routine.SqlRoutineAnalyzer.getLanguageName;
import static io.trino.sql.routine.SqlRoutineAnalyzer.getProperties;
import static io.trino.sql.routine.SqlRoutineAnalyzer.isRunAsInvoker;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class LanguageFunctionManager
        implements LanguageFunctionProvider
{
    public static final String QUERY_LOCAL_SCHEMA = "$query";
    private static final String SQL_FUNCTION_PREFIX = "$trino_sql_";
    private final SqlParser parser;
    private final TypeManager typeManager;
    private final GroupProvider groupProvider;
    private final BlockEncodingSerde blockEncodingSerde;
    private final LanguageFunctionEngineManager engineManager;
    private PlannerContext plannerContext;
    private SqlRoutineAnalyzer analyzer;
    private SqlRoutinePlanner planner;
    private final Map<QueryId, QueryFunctions> queryFunctions = new ConcurrentHashMap<>();

    @Inject
    public LanguageFunctionManager(
            SqlParser parser,
            TypeManager typeManager,
            GroupProvider groupProvider,
            BlockEncodingSerde blockEncodingSerde,
            LanguageFunctionEngineManager engineManager)
    {
        this.parser = requireNonNull(parser, "parser is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.engineManager = requireNonNull(engineManager, "engineManager is null");
    }

    // There is a circular dependency between LanguageFunctionManager and MetadataManager.
    // To determine the dependencies of a language function, it must be analyzed, and that
    // requires the metadata manager to resolve functions. The metadata manager needs the
    // language function manager to resolve language functions.
    public synchronized void setPlannerContext(PlannerContext plannerContext)
    {
        checkState(this.plannerContext == null, "plannerContext already set");
        this.plannerContext = plannerContext;
        analyzer = new SqlRoutineAnalyzer(plannerContext, WarningCollector.NOOP);
        planner = new SqlRoutinePlanner(plannerContext);
    }

    public void tryRegisterQuery(Session session)
    {
        queryFunctions.putIfAbsent(session.getQueryId(), new QueryFunctions(session));
    }

    public void registerQuery(Session session)
    {
        boolean alreadyRegistered = queryFunctions.putIfAbsent(session.getQueryId(), new QueryFunctions(session)) != null;
        if (alreadyRegistered) {
            throw new IllegalStateException("Query already registered: " + session.getQueryId());
        }
    }

    public void unregisterQuery(Session session)
    {
        queryFunctions.remove(session.getQueryId());
    }

    @Override
    public void registerTask(TaskId taskId, Map<FunctionId, LanguageFunctionData> languageFunctions)
    {
        // the functions are already registered in the query, so we don't need to do anything here
    }

    @Override
    public void unregisterTask(TaskId taskId) {}

    private QueryFunctions getQueryFunctions(Session session)
    {
        QueryFunctions queryFunctions = this.queryFunctions.get(session.getQueryId());
        if (queryFunctions == null) {
            throw new IllegalStateException("Query not registered: " + session.getQueryId());
        }
        return queryFunctions;
    }

    public List<FunctionMetadata> listFunctions(Session session, Collection<LanguageFunction> languageFunctions)
    {
        return languageFunctions.stream()
                .map(LanguageFunction::sql)
                .map(sql -> extractFunctionMetadata(createSqlLanguageFunctionId(session.getQueryId(), sql), parser.createFunctionSpecification(sql)))
                .collect(toImmutableList());
    }

    public List<FunctionMetadata> getFunctions(Session session, CatalogHandle catalogHandle, SchemaFunctionName name, LanguageFunctionLoader languageFunctionLoader, RunAsIdentityLoader identityLoader)
    {
        return getQueryFunctions(session).getFunctions(catalogHandle, name, languageFunctionLoader, identityLoader);
    }

    public FunctionMetadata getFunctionMetadata(Session session, FunctionId functionId)
    {
        return getQueryFunctions(session).getFunctionMetadata(functionId);
    }

    public FunctionId analyzeAndPlan(Session session, FunctionId functionId, AccessControl accessControl)
    {
        return getQueryFunctions(session).analyzeAndPlan(functionId, accessControl);
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionId functionId, InvocationConvention invocationConvention, FunctionManager functionManager)
    {
        // any resolved function in any query is guaranteed to have the same behavior, so we can use any query to get the implementation
        return queryFunctions.values().stream()
                .map(queryFunctions -> queryFunctions.specialize(functionId, invocationConvention, functionManager))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown function implementation: " + functionId));
    }

    public Map<FunctionId, LanguageFunctionData> serializeFunctionsForWorkers(Session session)
    {
        return getQueryFunctions(session).serializeFunctionsForWorkers();
    }

    public void verifyForCreate(Session session, FunctionSpecification function, FunctionManager functionManager, AccessControl accessControl)
    {
        getQueryFunctions(session).verifyForCreate(function, functionManager, accessControl);
    }

    public void addInlineFunction(Session session, FunctionSpecification function, AccessControl accessControl)
    {
        getQueryFunctions(session).addInlineFunction(function, accessControl);
    }

    public interface LanguageFunctionLoader
    {
        Collection<LanguageFunction> getLanguageFunction(ConnectorSession session, SchemaFunctionName name);
    }

    public interface RunAsIdentityLoader
    {
        Identity getFunctionRunAsIdentity(Optional<String> owner);
    }

    public static boolean isInlineFunction(CatalogSchemaFunctionName functionName)
    {
        return functionName.getCatalogName().equals(GlobalSystemConnector.NAME) && functionName.getSchemaName().equals(QUERY_LOCAL_SCHEMA);
    }

    public static boolean isTrinoSqlLanguageFunction(FunctionId functionId)
    {
        return functionId.toString().startsWith(SQL_FUNCTION_PREFIX);
    }

    private static FunctionId createSqlLanguageFunctionId(QueryId queryId, String sql)
    {
        // QueryId is added to the FunctionID to ensure it is unique across queries.
        // After the function is analyzed and planned, the FunctionId is replaced with a hash of the IrRoutine.
        String hash = Hashing.sha256().hashUnencodedChars(sql).toString();
        return new FunctionId(SQL_FUNCTION_PREFIX + queryId + "_" + hash);
    }

    public String getSignatureToken(List<ParameterDeclaration> parameters)
    {
        return parameters.stream()
                .map(ParameterDeclaration::getType)
                .map(TypeSignatureTranslator::toTypeSignature)
                .map(typeManager::getType)
                .map(Type::getTypeId)
                .map(TypeId::getId)
                .collect(joining(",", "(", ")"));
    }

    public List<Property> materializeFunctionProperties(Session session, FunctionSpecification function, Map<NodeRef<Parameter>, Expression> parameters, AccessControl accessControl)
    {
        LanguageFunctionEngine engine = getLanguageFunctionEngine(function);
        return toSqlProperties(
                "function language " + engine.getLanguage(),
                INVALID_FUNCTION_PROPERTY,
                evaluateFunctionProperties(session, function, parameters, accessControl),
                engine.getFunctionProperties());
    }

    private Map<String, Object> evaluateFunctionProperties(Session session, FunctionSpecification function, Map<NodeRef<Parameter>, Expression> parameters, AccessControl accessControl)
    {
        LanguageFunctionEngine engine = getLanguageFunctionEngine(function);
        Map<String, Optional<Object>> nullableValues = evaluateProperties(
                getProperties(function),
                session,
                plannerContext,
                accessControl,
                parameters,
                true,
                Maps.uniqueIndex(engine.getFunctionProperties(), PropertyMetadata::getName),
                INVALID_FUNCTION_PROPERTY,
                "function language %s property".formatted(engine.getLanguage()));

        return nullableValues.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().orElseThrow()));
    }

    private LanguageFunctionEngine getLanguageFunctionEngine(FunctionSpecification function)
    {
        String language = getLanguageName(function);
        return engineManager.getLanguageFunctionEngine(language)
                .orElseThrow(() -> {
                    Node node = getLanguage(function).map(Node.class::cast).orElse(function);
                    return semanticException(NOT_SUPPORTED, node, "Unsupported function language: %s", language);
                });
    }

    private class QueryFunctions
    {
        private final Session session;
        private final Map<FunctionKey, FunctionListing> functionListing = new ConcurrentHashMap<>();
        private final Map<FunctionId, LanguageFunctionImplementation> implementationsById = new ConcurrentHashMap<>();
        private final Map<FunctionId, LanguageFunctionData> usedFunctions = new ConcurrentHashMap<>();

        public QueryFunctions(Session session)
        {
            this.session = session;
        }

        public void verifyForCreate(FunctionSpecification function, FunctionManager functionManager, AccessControl accessControl)
        {
            implementationWithoutSecurity(session.getQueryId(), function).verifyForCreate(functionManager, accessControl);
        }

        public void addInlineFunction(FunctionSpecification function, AccessControl accessControl)
        {
            LanguageFunctionImplementation implementation = implementationWithoutSecurity(session.getQueryId(), function);
            FunctionMetadata metadata = implementation.getFunctionMetadata();
            implementationsById.put(metadata.getFunctionId(), implementation);
            SchemaFunctionName name = new SchemaFunctionName(QUERY_LOCAL_SCHEMA, metadata.getCanonicalName());
            getFunctionListing(GlobalSystemConnector.CATALOG_HANDLE, name).addFunction(metadata);

            // enforce that functions may only call already registered functions and prevent recursive calls
            implementation.analyzeAndPlan(accessControl);
        }

        public synchronized List<FunctionMetadata> getFunctions(CatalogHandle catalogHandle, SchemaFunctionName name, LanguageFunctionLoader languageFunctionLoader, RunAsIdentityLoader identityLoader)
        {
            return getFunctionListing(catalogHandle, name).getFunctions(languageFunctionLoader, identityLoader);
        }

        public FunctionId analyzeAndPlan(FunctionId functionId, AccessControl accessControl)
        {
            LanguageFunctionImplementation function = implementationsById.get(functionId);
            checkArgument(function != null, "Unknown function implementation: %s", functionId);

            // verify the function and check permissions of nested function calls
            function.analyzeAndPlan(accessControl);

            LanguageFunctionData data = function.getFunctionData();
            FunctionId resolvedFunctionId = function.getResolvedFunctionId();

            // mark the function as used, so it is serialized for workers
            usedFunctions.put(resolvedFunctionId, data);
            return resolvedFunctionId;
        }

        public Optional<ScalarFunctionImplementation> specialize(FunctionId functionId, InvocationConvention invocationConvention, FunctionManager functionManager)
        {
            LanguageFunctionData data = usedFunctions.get(functionId);
            if (data == null) {
                return Optional.empty();
            }

            if (data.definition().isPresent()) {
                LanguageFunctionDefinition definition = data.definition().get();

                LanguageFunctionEngine engine = engineManager.getLanguageFunctionEngine(definition.language())
                        .orElseThrow(() -> new IllegalStateException("No language function engine for language: " + definition.language()));

                return Optional.of(engine.getScalarFunctionImplementation(
                        definition.returnType(),
                        definition.argumentTypes(),
                        definition.definition(),
                        definition.properties(),
                        invocationConvention));
            }

            IrRoutine routine = data.irRoutine().orElseThrow();
            SpecializedSqlScalarFunction function = new SqlRoutineCompiler(functionManager).compile(routine);
            return Optional.of(function.getScalarFunctionImplementation(invocationConvention));
        }

        public FunctionMetadata getFunctionMetadata(FunctionId functionId)
        {
            LanguageFunctionImplementation function = implementationsById.get(functionId);
            checkArgument(function != null, "Unknown function implementation: %s", functionId);
            return function.getFunctionMetadata();
        }

        public Map<FunctionId, LanguageFunctionData> serializeFunctionsForWorkers()
        {
            return ImmutableMap.copyOf(usedFunctions);
        }

        private FunctionListing getFunctionListing(CatalogHandle catalogHandle, SchemaFunctionName name)
        {
            return functionListing.computeIfAbsent(new FunctionKey(catalogHandle, name), FunctionListing::new);
        }

        private record FunctionKey(CatalogHandle catalogHandle, SchemaFunctionName name) {}

        private class FunctionListing
        {
            private final CatalogHandle catalogHandle;
            private final SchemaFunctionName name;
            private final List<FunctionMetadata> functions = new ArrayList<>();
            private boolean loaded;

            public FunctionListing(FunctionKey key)
            {
                catalogHandle = key.catalogHandle();
                name = key.name();
            }

            public synchronized void addFunction(FunctionMetadata function)
            {
                functions.add(function);
                loaded = true;
            }

            public synchronized List<FunctionMetadata> getFunctions(LanguageFunctionLoader languageFunctionLoader, RunAsIdentityLoader identityLoader)
            {
                if (loaded) {
                    return ImmutableList.copyOf(functions);
                }
                loaded = true;

                List<LanguageFunctionImplementation> implementations = languageFunctionLoader.getLanguageFunction(session.toConnectorSession(), name).stream()
                        .map(function -> implementationWithSecurity(session.getQueryId(), function.sql(), function.path(), function.owner(), identityLoader))
                        .collect(toImmutableList());

                // verify all names are correct
                // Note: language functions don't have aliases
                Set<String> names = implementations.stream()
                        .map(function -> function.getFunctionMetadata().getCanonicalName())
                        .collect(toImmutableSet());
                if (!names.isEmpty() && !names.equals(Set.of(name.getFunctionName()))) {
                    throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Catalog %s returned functions named %s when listing functions named %s".formatted(catalogHandle.getCatalogName(), names, name));
                }

                // add the functions to this listing
                implementations.forEach(implementation -> functions.add(implementation.getFunctionMetadata()));

                // add the functions to the catalog index
                implementations.forEach(processedFunction -> implementationsById.put(processedFunction.getFunctionMetadata().getFunctionId(), processedFunction));

                return ImmutableList.copyOf(functions);
            }
        }

        private LanguageFunctionImplementation implementationWithoutSecurity(QueryId queryId, FunctionSpecification function)
        {
            // use the original path during function creation and for inline functions
            return new LanguageFunctionImplementation(
                    queryId,
                    formatSql(function),
                    function,
                    session.getPath(),
                    Optional.empty(),
                    Optional.empty());
        }

        private LanguageFunctionImplementation implementationWithSecurity(QueryId queryId, String sql, List<CatalogSchemaName> path, Optional<String> owner, RunAsIdentityLoader identityLoader)
        {
            // stored functions cannot see inline functions, so we need to rebuild the path
            return new LanguageFunctionImplementation(
                    queryId,
                    sql,
                    parser.createFunctionSpecification(sql),
                    session.getPath().forView(path),
                    owner,
                    Optional.of(identityLoader));
        }

        private class LanguageFunctionImplementation
        {
            private final FunctionMetadata functionMetadata;
            private final FunctionSpecification functionSpecification;
            private final SqlPath path;
            private final Optional<String> owner;
            private final Optional<RunAsIdentityLoader> identityLoader;
            private final String language;
            private final boolean engineFunction;
            private LanguageFunctionData data;
            private IrRoutine routine;
            private FunctionId resolvedFunctionId;
            private boolean analyzing;

            private LanguageFunctionImplementation(QueryId queryId, String sql, FunctionSpecification function, SqlPath path, Optional<String> owner, Optional<RunAsIdentityLoader> identityLoader)
            {
                this.functionSpecification = requireNonNull(function, "function is null");
                this.functionMetadata = extractFunctionMetadata(createSqlLanguageFunctionId(queryId, sql), functionSpecification);
                this.path = requireNonNull(path, "path is null");
                this.owner = requireNonNull(owner, "owner is null");
                this.identityLoader = requireNonNull(identityLoader, "identityLoader is null");
                this.language = getLanguageName(function);
                this.engineFunction = !language.equalsIgnoreCase("SQL");
            }

            public FunctionMetadata getFunctionMetadata()
            {
                return functionMetadata;
            }

            public synchronized void verifyForCreate(FunctionManager functionManager, AccessControl accessControl)
            {
                checkState(identityLoader.isEmpty(), "create should not enforce security");
                analyzeAndPlan(accessControl);
                if (!engineFunction) {
                    new SqlRoutineCompiler(functionManager).compile(routine);
                }
            }

            private synchronized void analyzeAndPlan(AccessControl accessControl)
            {
                if (data != null) {
                    return;
                }

                if (engineFunction) {
                    data = LanguageFunctionData.ofDefinition(analyzeEngineFunction(functionContext(accessControl)));
                    resolvedFunctionId = functionMetadata.getFunctionId();
                    return;
                }

                if (analyzing) {
                    String error = "Recursive language functions are not supported: " + nameSignature();
                    if (originalAst()) {
                        throw new TrinoException(NOT_SUPPORTED, extractLocation(functionSpecification), error, null);
                    }
                    throw new LanguageFunctionAnalysisException(NOT_SUPPORTED, error);
                }

                analyzing = true;

                SqlRoutineAnalysis analysis = analyzeSqlFunction(functionContext(accessControl));
                routine = planner.planSqlFunction(session, analysis);
                data = LanguageFunctionData.ofIrRoutine(routine);

                Hasher hasher = Hashing.sha256().newHasher();
                SqlRoutineHash.hash(routine, hasher, blockEncodingSerde);
                resolvedFunctionId = new FunctionId(SQL_FUNCTION_PREFIX + hasher.hash());

                analyzing = false;
            }

            private SqlRoutineAnalysis analyzeSqlFunction(FunctionContext context)
            {
                try {
                    return analyzer.analyze(context.session(), context.accessControl(), functionSpecification);
                }
                catch (TrinoException e) {
                    if (originalAst() || (e instanceof LanguageFunctionAnalysisException)) {
                        throw e;
                    }
                    if (e.getErrorCode().getType() == USER_ERROR) {
                        throw new TrinoException(e::getErrorCode, e.getRawMessage(), e);
                    }
                    throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Error analyzing stored function: " + nameSignature(), e);
                }
            }

            private LanguageFunctionDefinition analyzeEngineFunction(FunctionContext context)
            {
                LanguageFunctionDefinition definition = engineFunctionDefinition(context);
                validateEngineFunction(definition);
                return definition;
            }

            private LanguageFunctionDefinition engineFunctionDefinition(FunctionContext context)
            {
                Type returnType = typeManager.getType(functionMetadata.getSignature().getReturnType());

                List<Type> argumentTypes = functionMetadata.getSignature().getArgumentTypes().stream()
                        .map(typeManager::getType)
                        .collect(toImmutableList());

                String definition = functionSpecification.getDefinition().orElseThrow().getValue();

                Map<String, Object> properties = engineFunctionProperties(context);

                return new LanguageFunctionDefinition(language, returnType, argumentTypes, definition, properties);
            }

            private Map<String, Object> engineFunctionProperties(FunctionContext context)
            {
                try {
                    return evaluateFunctionProperties(context.session(), functionSpecification, Map.of(), context.accessControl());
                }
                catch (TrinoException e) {
                    if (originalAst()) {
                        throw e;
                    }
                    throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Error analyzing stored function: " + nameSignature(), e);
                }
            }

            private void validateEngineFunction(LanguageFunctionDefinition definition)
            {
                try {
                    LanguageFunctionEngine engine = getLanguageFunctionEngine(functionSpecification);
                    engine.validateScalarFunction(definition.returnType(), definition.argumentTypes(), definition.definition(), definition.properties());
                }
                catch (TrinoException e) {
                    if (originalAst()) {
                        String message = "Invalid function '%s': %s".formatted(functionMetadata.getCanonicalName(), e.getMessage());
                        throw new TrinoException(e::getErrorCode, extractLocation(functionSpecification), message, e);
                    }
                    throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Error validating stored function: " + nameSignature(), e);
                }
            }

            private String nameSignature()
            {
                return functionMetadata.getCanonicalName() + functionMetadata.getSignature();
            }

            public synchronized LanguageFunctionData getFunctionData()
            {
                checkState(data != null, "function not yet analyzed");
                return data;
            }

            public synchronized FunctionId getResolvedFunctionId()
            {
                checkState(resolvedFunctionId != null, "function not yet analyzed");
                return resolvedFunctionId;
            }

            private FunctionContext functionContext(AccessControl accessControl)
            {
                if (identityLoader.isEmpty() || isRunAsInvoker(functionSpecification)) {
                    Session functionSession = createFunctionSession(session.getIdentity());
                    return new FunctionContext(functionSession, accessControl);
                }

                Identity identity = identityLoader.get().getFunctionRunAsIdentity(owner);

                Identity newIdentity = Identity.from(identity)
                        .withGroups(groupProvider.getGroups(identity.getUser()))
                        .build();

                Session functionSession = createFunctionSession(newIdentity);

                if (!identity.getUser().equals(session.getUser())) {
                    accessControl = new ViewAccessControl(accessControl);
                }

                return new FunctionContext(functionSession, accessControl);
            }

            private Session createFunctionSession(Identity identity)
            {
                return session.createViewSession(Optional.empty(), Optional.empty(), identity, path);
            }

            private boolean originalAst()
            {
                // The identity loader is empty for inline functions or function creation,
                // which means that we have the original AST.
                return identityLoader.isEmpty();
            }

            private record FunctionContext(Session session, AccessControl accessControl) {}
        }
    }
}
