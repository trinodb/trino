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
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Resolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ResolverManager
{
    private final Map<String, Resolver> resolvers = new ConcurrentHashMap<>();
    private final Map<String, List<String>> queries = new ConcurrentHashMap<>();
    private final Map<String, Optional<Function<Identifier, String>>> canonicalizers = new ConcurrentHashMap<>();

    public ResolverManager() {}

    public Optional<String> lastResolverName(Session session)
    {
        return hasSession(session) ? Optional.of(getQuery(session).getLast()) : Optional.empty();
    }

    public void setResolver(Session session, String catalog, BiFunction<Session, String, Resolver> factory)
    {
        checkResolvers(session, catalog, factory);
        if (!hasSession(session)) {
            queries.put(session.getQueryId().id(), new ArrayList<>());
        }
        getQuery(session).add(catalog);
    }

    public Resolver getResolver(Session session, String catalog, BiFunction<Session, String, Resolver> factory)
    {
        setResolver(session, catalog, factory);
        return resolvers.get(catalog);
    }

    public Resolver getDefaultResolver(Session session, BiFunction<Session, String, Resolver> factory)
    {
        if (hasSession(session)) {
            return resolvers.get(getQuery(session).getLast());
        }
        return getResolver(session, GlobalSystemConnector.NAME, factory);
    }

    public Optional<Resolver> getResolver(Session session, BiFunction<Session, String, Resolver> factory)
    {
        Optional<Resolver> resolver = Optional.empty();
        if (hasSession(session)) {
            List<String> catalogs = getQuery(session);
            resolver = Optional.of(resolvers.get(catalogs.getLast()));
        }
        else if (session.getCatalog().isPresent()) {
            System.out.println("ResolverManager.getResolver() catalog: " + session.getCatalog().get() + " WARNING ***********************************");
            return Optional.of(getResolver(session, session.getCatalog().get(), factory));
        }
        else {
            System.out.println("ResolverManager.getResolver() ERROR ***********************************");
            System.out.println("ResolverManager.getResolver() stacktrace: " + Arrays.toString(Thread.currentThread().getStackTrace()).replace(',', '\n'));
        }
        return resolver;
    }

    public boolean hasSession(Session session)
    {
        return queries.containsKey(session.getQueryId().id());
    }

    private boolean hasResolver(String catalog)
    {
        return resolvers.containsKey(catalog);
    }

    private List<String> getQuery(Session session)
    {
        return queries.get(session.getQueryId().id());
    }

    private void checkResolvers(Session session, String catalog, BiFunction<Session, String, Resolver> factory)
    {
        if (!hasResolver(catalog)) {
            resolvers.put(catalog, factory.apply(session, catalog));
        }
    }

    public void setCanonicalizer(Session session, Optional<Function<Identifier, String>> canonicalizer)
    {
        canonicalizers.put(session.getQueryId().id(), canonicalizer);
    }

    public Optional<Function<Identifier, String>> getCanonicalizer(Session session)
    {
        return canonicalizers.getOrDefault(session.getQueryId().id(), Optional.empty());
    }
}
