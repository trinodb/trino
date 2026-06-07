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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.sql.analyzer.FieldId;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.Resolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ResolverManager
{
    public static BiFunction<String, Boolean, String> getIdentityCanonicalizer()
    {
        return (value, _) -> value;
    }

    public static BiFunction<String, Boolean, String> getLowerCaseCanonicalizer()
    {
        return (value, delimited) -> delimited ? value : value.toLowerCase(ENGLISH);
    }

    public static BiFunction<String, Boolean, String> getUpperCaseCanonicalizer()
    {
        return (value, delimited) -> delimited ? value : value.toUpperCase(ENGLISH);
    }

    public static Resolver getResolver(String catalog, BiFunction<String, Boolean, String> canonicalizer)
    {
        return new Resolver(
                catalog,
                canonicalizer,
                (value, _) -> value,
                value -> !Identifier.isValidIdentifier(value));
    }

    private final Resolver DEFAULT_RESOLVER = getResolver("DEFAULT_RESOLVER", getUpperCaseCanonicalizer());
    private final Resolver SYSTEM_RESOLVER = getResolver(GlobalSystemConnector.NAME, getLowerCaseCanonicalizer());
    private final Resolver WITH_RESOLVER = getResolver("WITH_RESOLVER", getUpperCaseCanonicalizer());
    private final Resolver PARTITION_RESOLVER = getResolver("PARTITION_RESOLVER", getIdentityCanonicalizer());

    private final Map<String, Resolver> resolvers = new ConcurrentHashMap<>();
    private final Map<String, String> queries = new ConcurrentHashMap<>();
    private final Optional<BiFunction<Session, String, Optional<Resolver>>> factory;

    public ResolverManager()
    {
        this(Optional.empty());
    }

    public ResolverManager(BiFunction<Session, String, Optional<Resolver>> factory)
    {
        this(Optional.of(factory));
    }

    private ResolverManager(Optional<BiFunction<Session, String, Optional<Resolver>>> factory)
    {
        this.factory = requireNonNull(factory, "factory is null");
        addResolver(SYSTEM_RESOLVER.getCatalog(), SYSTEM_RESOLVER);
    }

    public ResolverManager addResolver(String catalog, BiFunction<String, Boolean, String> canonicalizer)
    {
        addResolver(catalog, getResolver(catalog, canonicalizer));
        return this;
    }

    public Resolver getResolver(Session session, String catalog)
    {
        if (!hasResolver(catalog)) {
            if (factory.isPresent()) {
                Optional<Resolver> resolver = factory.get().apply(session, catalog);
                if (resolver.isPresent()) {
                    addResolver(catalog, resolver.get());
                    return resolver.get();
                }
            }
            throw new TrinoException(CATALOG_NOT_FOUND, format("Unable to create Resolver for catalog '%s'", catalog));
        }
        return resolvers.get(catalog);
    }

    public Map<String, Function<Identifier, String>> getCanonicalizers()
    {
        ImmutableMap.Builder<String, Function<Identifier, String>> canonicalizers = ImmutableMap.builder();
        resolvers.forEach((catalog, resolver) -> canonicalizers.put(catalog, resolver.getCanonicalizer()));
        return canonicalizers.build();
    }

    public void setQueryResolver(Session session, Resolver resolver)
    {
        if (!hasResolver(resolver.getCatalog())) {
            resolvers.put(resolver.getCatalog(), resolver);
        }
        queries.put(session.getQueryId().id(), resolver.getCatalog());
    }

    public Resolver getQueryResolver(Session session, Optional<Scope> scope)
    {
        if (scope.isPresent() && scope.get().getResolver().isPresent()) {
            return scope.get().getResolver().get();
        }
        return getQueryResolver(session.getQueryId().id());
    }

    public Resolver getPartitionResolver()
    {
        return PARTITION_RESOLVER;
    }

    public Resolver getWithResolver()
    {
        return WITH_RESOLVER;
    }

    private Resolver getQueryResolver(String queryId)
    {
        if (queries.containsKey(queryId) && hasResolver(queries.get(queryId))) {
            return resolvers.get(queries.get(queryId));
        }
        return DEFAULT_RESOLVER;
    }

    private boolean hasResolver(String catalog)
    {
        return resolvers.containsKey(catalog);
    }

    private void addResolver(String catalog, Resolver resolver)
    {
        resolvers.put(catalog, resolver);
    }
}
