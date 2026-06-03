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

import io.trino.Session;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Resolver;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

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
    private final Resolver WITH_RESOLVER = getResolver("WITH_RESOLVER", getUpperCaseCanonicalizer());
    private final Resolver PARTITION_RESOLVER = getResolver("PARTITION_RESOLVER", getIdentityCanonicalizer());

    private final Map<String, Resolver> resolvers = new ConcurrentHashMap<>();
    private final Map<String, String> queries = new ConcurrentHashMap<>();
    private final BiFunction<Session, String, Resolver> factory;

    public ResolverManager(BiFunction<Session, String, Resolver> factory)
    {
        this.factory = requireNonNull(factory, "factory is null");
    }

    public void setResolver(String catalog, BiFunction<String, Boolean, String> canonicalizer)
    {
        this.resolvers.put(catalog, getResolver(catalog, canonicalizer));
    }

    public Resolver getResolver(Session session, String catalog)
    {
        if (!hasResolver(catalog)) {
            resolvers.put(catalog, factory.apply(session, catalog));
        }
        return resolvers.get(catalog);
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
}
