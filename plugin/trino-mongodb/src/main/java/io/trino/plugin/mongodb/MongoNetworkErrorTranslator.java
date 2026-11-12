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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoStalePrimaryException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import io.trino.spi.TrinoException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.plugin.mongodb.MongoErrorCode.MONGODB_CLUSTER_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link MongoClient} so that failures to communicate with the MongoDB cluster
 * (network errors, timeouts, unavailable or failing-over servers) surface as external errors,
 * regardless of which driver call or lazily-iterated cursor they are thrown from.
 */
final class MongoNetworkErrorTranslator
{
    // Driver types whose methods talk to the cluster, directly or through objects they return
    private static final List<Class<?>> TRANSLATED_TYPES = ImmutableList.of(
            MongoCluster.class,
            MongoDatabase.class,
            MongoCollection.class,
            MongoIterable.class,
            MongoCursor.class);

    private static final List<Class<? extends Throwable>> NETWORK_ERRORS = ImmutableList.of(
            MongoSocketException.class,
            MongoTimeoutException.class,
            MongoConnectionPoolClearedException.class,
            MongoServerUnavailableException.class,
            MongoNotPrimaryException.class,
            MongoNodeIsRecoveringException.class,
            MongoStalePrimaryException.class);

    private MongoNetworkErrorTranslator() {}

    public static MongoClient translateNetworkErrors(MongoClient client)
    {
        return translating(MongoClient.class, client);
    }

    private static <T> T translating(Class<T> type, T delegate)
    {
        requireNonNull(delegate, "delegate is null");
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.isDefault()) {
                // Run interface default methods (e.g. Iterable.spliterator) on the proxy, bypassing any override in the delegate,
                // so that the calls they make, including lazy cursor iteration, are translated too
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            Object result;
            try {
                result = method.invoke(delegate, args);
            }
            catch (InvocationTargetException e) {
                throw translate(e.getCause());
            }
            Class<?> returnType = method.getReturnType();
            if (result != null && returnType.isInterface() && TRANSLATED_TYPES.stream().anyMatch(translatedType -> translatedType.isAssignableFrom(returnType))) {
                return wrap(returnType, result);
            }
            return result;
        };
        return newProxy(type, handler);
    }

    @SuppressWarnings("unchecked")
    private static <T> T wrap(Class<T> type, Object delegate)
    {
        return translating(type, (T) delegate);
    }

    private static Throwable translate(Throwable throwable)
    {
        if (NETWORK_ERRORS.stream().anyMatch(networkError -> networkError.isInstance(throwable))) {
            return new TrinoException(MONGODB_CLUSTER_ERROR, throwable);
        }
        return throwable;
    }
}
