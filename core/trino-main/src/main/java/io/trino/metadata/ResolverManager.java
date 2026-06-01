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
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Resolver;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static java.util.Locale.ENGLISH;

public class ResolverManager
{
    private final Resolver DEFAULT_RESOLVER = new Resolver(
            "DEFAULT_RESOLVER",
            (value, delimited) -> delimited ? value : value.toUpperCase(ENGLISH),
            (value, _) -> value,
            value -> !Identifier.isValidIdentifier(value));

    private final Resolver WITH_RESOLVER = new Resolver(
            "WITH_RESOLVER",
            (value, delimited) -> delimited ? value : value.toUpperCase(ENGLISH),
            (value, _) -> value,
            value -> !Identifier.isValidIdentifier(value));

    private final Map<String, Resolver> resolvers = new ConcurrentHashMap<>();
    private final Map<String, String> queries = new ConcurrentHashMap<>();

    public ResolverManager() {}

    public Resolver getResolver(Session session, String catalog, BiFunction<Session, String, Resolver> factory)
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

    public Optional<Resolver> getQueryResolver(String queryId)
    {
        if (queries.containsKey(queryId) && hasResolver(queries.get(queryId))) {
            return Optional.of(resolvers.get(queries.get(queryId)));
        }
        return Optional.empty();
    }

    public Resolver getWithResolver()
    {
        return WITH_RESOLVER;
    }

    public Resolver getDefaultResolver()
    {
        return DEFAULT_RESOLVER;
    }

    private boolean hasResolver(String catalog)
    {
        return resolvers.containsKey(catalog);
    }
}
