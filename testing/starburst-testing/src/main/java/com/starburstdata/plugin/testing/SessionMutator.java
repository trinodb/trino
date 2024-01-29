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
package com.starburstdata.plugin.testing;

import io.trino.Session;
import org.testng.Assert.ThrowingRunnable;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SessionMutator
{
    private static final Function<Session, Session> IDENTITY = Function.identity();

    private final Supplier<Session> baseSessionSupplier;
    private final ThreadLocal<Function<Session, Session>> sessionMutatorsThreadLocal = ThreadLocal.withInitial(() -> IDENTITY);

    public SessionMutator(Supplier<Session> baseSessionSupplier)
    {
        this.baseSessionSupplier = requireNonNull(baseSessionSupplier, "baseSessionSupplier is null");
    }

    public Session getSession()
    {
        Function<Session, Session> sessionMutator = sessionMutatorsThreadLocal.get();
        Session baseSession = baseSessionSupplier.get();
        return sessionMutator.apply(baseSession);
    }

    public class CallConsumer
    {
        private final Function<Session, Session> sessionMutator;

        public CallConsumer(Function<Session, Session> sessionMutator)
        {
            this.sessionMutator = requireNonNull(sessionMutator, "sessionMutator is null");
        }

        public void call(ThrowingRunnable action)
        {
            checkState(sessionMutatorsThreadLocal.get() == IDENTITY, "Session mutator already set");
            sessionMutatorsThreadLocal.set(sessionMutator);
            try {
                action.run();
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
            finally {
                sessionMutatorsThreadLocal.remove();
            }
        }
    }
}
