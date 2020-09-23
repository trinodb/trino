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
package io.prestosql.jdbc;

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.client.auth.external.DesktopBrowserRedirectHandler;
import io.prestosql.client.auth.external.RedirectHandler;

import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class RedirectHandlerFactory
{
    private static final AtomicReference<Supplier<RedirectHandler>> handlerFactory = new AtomicReference<>(DesktopBrowserRedirectHandler::new);

    private RedirectHandlerFactory()
    {
    }

    static RedirectHandler create()
    {
        return handlerFactory.get()
                .get();
    }

    @VisibleForTesting
    static void setHandlerFactory(Supplier<RedirectHandler> next)
    {
        Supplier<RedirectHandler> current = handlerFactory.get();
        if (!handlerFactory.compareAndSet(current, next)) {
            throw new ConcurrentModificationException("Concurrent modification of RedirectHandlerFactory detected. Use @Test(singleThreaded = true)");
        }
    }
}
