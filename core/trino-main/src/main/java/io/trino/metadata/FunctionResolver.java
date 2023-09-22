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
import io.trino.Session;
import io.trino.metadata.FunctionBinder.CatalogFunctionBinding;
import io.trino.spi.TrinoException;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.TypeManager;
import io.trino.sql.SqlPathElement;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.metadata.FunctionBinder.functionNotFound;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.WINDOW;

public class FunctionResolver
{
    private final FunctionBinder functionBinder;

    public FunctionResolver(Metadata metadata, TypeManager typeManager)
    {
        this.functionBinder = new FunctionBinder(metadata, typeManager);
    }

    boolean isAggregationFunction(Session session, QualifiedName name, Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            if (!candidates.isEmpty()) {
                return candidates.stream()
                        .map(CatalogFunctionMetadata::functionMetadata)
                        .map(FunctionMetadata::getKind)
                        .anyMatch(AGGREGATE::equals);
            }
        }
        return false;
    }

    boolean isWindowFunction(Session session, QualifiedName name, Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            if (!candidates.isEmpty()) {
                return candidates.stream()
                        .map(CatalogFunctionMetadata::functionMetadata)
                        .map(FunctionMetadata::getKind)
                        .anyMatch(WINDOW::equals);
            }
        }
        return false;
    }

    CatalogFunctionBinding resolveCoercion(Signature signature, Collection<CatalogFunctionMetadata> candidates)
    {
        return functionBinder.bindCoercion(signature, candidates);
    }

    CatalogFunctionBinding resolveFunction(
            Session session,
            QualifiedName name,
            List<TypeSignatureProvider> parameterTypes,
            Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        ImmutableList.Builder<CatalogFunctionMetadata> allCandidates = ImmutableList.builder();
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            Optional<CatalogFunctionBinding> match = functionBinder.tryBindFunction(parameterTypes, candidates);
            if (match.isPresent()) {
                return match.get();
            }
            allCandidates.addAll(candidates);
        }

        List<CatalogFunctionMetadata> candidates = allCandidates.build();
        throw functionNotFound(name.toString(), parameterTypes, candidates);
    }

    CatalogFunctionBinding resolveFullyQualifiedFunction(
            CatalogSchemaFunctionName name,
            List<TypeSignatureProvider> parameterTypes,
            Collection<CatalogFunctionMetadata> candidates)
    {
        return functionBinder.bindFunction(parameterTypes, candidates, name.toString());
    }

    public static List<CatalogSchemaFunctionName> toPath(Session session, QualifiedName name)
    {
        List<String> parts = name.getParts();
        if (parts.size() > 3) {
            throw new TrinoException(FUNCTION_NOT_FOUND, "Invalid function name: " + name);
        }
        if (parts.size() == 3) {
            return ImmutableList.of(new CatalogSchemaFunctionName(parts.get(0), parts.get(1), parts.get(2)));
        }

        if (parts.size() == 2) {
            String currentCatalog = session.getCatalog()
                    .orElseThrow(() -> new TrinoException(MISSING_CATALOG_NAME, "Session default catalog must be set to resolve a partial function name: " + name));
            return ImmutableList.of(new CatalogSchemaFunctionName(currentCatalog, parts.get(0), parts.get(1)));
        }

        ImmutableList.Builder<CatalogSchemaFunctionName> names = ImmutableList.builder();

        // global namespace
        names.add(builtinFunctionName(parts.get(0)));

        // add resolved path items
        for (SqlPathElement sqlPathElement : session.getPath().getParsedPath()) {
            String catalog = sqlPathElement.getCatalog().map(Identifier::getCanonicalValue).or(session::getCatalog)
                    .orElseThrow(() -> new TrinoException(MISSING_CATALOG_NAME, "Session default catalog must be set to resolve a partial function name: " + name));
            names.add(new CatalogSchemaFunctionName(catalog, sqlPathElement.getSchema().getCanonicalValue(), parts.get(0)));
        }
        return names.build();
    }
}
