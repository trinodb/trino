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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.function.LanguageFunction;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateFunction;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.routine.SqlRoutineAnalyzer.isRunAsInvoker;
import static java.util.Objects.requireNonNull;

public class CreateFunctionTask
        implements DataDefinitionTask<CreateFunction>
{
    private final Optional<CatalogSchemaName> defaultFunctionSchema;
    private final SqlParser sqlParser;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final AccessControl accessControl;
    private final LanguageFunctionManager languageFunctionManager;

    @Inject
    public CreateFunctionTask(
            SqlEnvironmentConfig sqlEnvironmentConfig,
            SqlParser sqlParser,
            Metadata metadata,
            FunctionManager functionManager,
            AccessControl accessControl,
            LanguageFunctionManager languageFunctionManager)
    {
        this.defaultFunctionSchema = defaultFunctionSchema(sqlEnvironmentConfig);
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.languageFunctionManager = requireNonNull(languageFunctionManager, "languageFunctionManager is null");
    }

    @Override
    public String getName()
    {
        return "CREATE FUNCTION";
    }

    @Override
    public ListenableFuture<Void> execute(CreateFunction statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        FunctionSpecification function = statement.getSpecification();
        QualifiedObjectName name = qualifiedFunctionName(defaultFunctionSchema, statement, function.getName());

        accessControl.checkCanCreateFunction(session.toSecurityContext(), name);

        String formatted = formatSql(function);
        verifyFormattedFunction(formatted, function);

        languageFunctionManager.verifyForCreate(session, formatted, functionManager, accessControl);

        String signatureToken = languageFunctionManager.getSignatureToken(function.getParameters());

        // system path elements currently are not stored
        List<CatalogSchemaName> path = session.getPath().getPath().stream()
                .filter(element -> !element.getCatalogName().equals(GlobalSystemConnector.NAME))
                .toList();

        Optional<String> owner = isRunAsInvoker(function) ? Optional.empty() : Optional.of(session.getUser());

        LanguageFunction languageFunction = new LanguageFunction(signatureToken, formatted, path, owner);

        boolean replace = false;
        if (metadata.languageFunctionExists(session, name, signatureToken)) {
            if (!statement.isReplace()) {
                throw semanticException(ALREADY_EXISTS, statement, "Function already exists");
            }
            accessControl.checkCanDropFunction(session.toSecurityContext(), name);
            replace = true;
        }

        metadata.createLanguageFunction(session, name, languageFunction, replace);

        return immediateVoidFuture();
    }

    private void verifyFormattedFunction(String sql, FunctionSpecification function)
    {
        try {
            FunctionSpecification parsed = sqlParser.createFunctionSpecification(sql);
            if (!function.equals(parsed)) {
                throw formattingFailure(null, "Function does not round-trip", function, sql);
            }
        }
        catch (ParsingException e) {
            throw formattingFailure(e, "Formatted function does not parse", function, sql);
        }
    }

    static Optional<CatalogSchemaName> defaultFunctionSchema(SqlEnvironmentConfig config)
    {
        return combine(config.getDefaultFunctionCatalog(), config.getDefaultFunctionSchema(), CatalogSchemaName::new);
    }

    static QualifiedObjectName qualifiedFunctionName(Optional<CatalogSchemaName> functionSchema, Node node, QualifiedName name)
    {
        List<String> parts = name.getParts();
        return switch (parts.size()) {
            case 1 -> {
                CatalogSchemaName schema = functionSchema.orElseThrow(() ->
                        semanticException(NOT_SUPPORTED, node, "Catalog and schema must be specified when function schema is not configured"));
                yield new QualifiedObjectName(schema.getCatalogName(), schema.getSchemaName(), parts.get(0));
            }
            case 2 -> throw semanticException(NOT_SUPPORTED, node, "Function name must be unqualified or fully qualified with catalog and schema");
            case 3 -> new QualifiedObjectName(parts.get(0), parts.get(1), parts.get(2));
            default -> throw semanticException(SYNTAX_ERROR, node, "Too many dots in function name: %s", name);
        };
    }

    private static TrinoException formattingFailure(Throwable cause, String message, FunctionSpecification function, String sql)
    {
        TrinoException exception = new TrinoException(GENERIC_INTERNAL_ERROR, message, cause);
        exception.addSuppressed(new RuntimeException("Function: " + function));
        exception.addSuppressed(new RuntimeException("Formatted: [%s]".formatted(sql)));
        return exception;
    }

    private static <T, U, R> Optional<R> combine(Optional<T> first, Optional<U> second, BiFunction<T, U, R> combiner)
    {
        return (first.isPresent() && second.isPresent())
                ? Optional.of(combiner.apply(first.get(), second.get()))
                : Optional.empty();
    }
}
