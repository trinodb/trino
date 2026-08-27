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
package io.trino.plugin.paimon;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A process-local Paimon lock used to exercise the atomic commit boundary against MinIO.
 */
public final class TestingCatalogLockFactory
        implements CatalogLockFactory
{
    private static final ConcurrentHashMap<String, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    @Override
    public String identifier()
    {
        return "trino-test";
    }

    @Override
    public CatalogLock createLock(CatalogLockContext context)
    {
        return new TestingCatalogLock();
    }

    private static final class TestingCatalogLock
            implements CatalogLock
    {
        @Override
        public <T> T runWithLock(String database, String table, Callable<T> callable)
                throws Exception
        {
            ReentrantLock lock = LOCKS.computeIfAbsent(database + "." + table, _ -> new ReentrantLock());
            lock.lock();
            try {
                return callable.call();
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {}
    }
}
