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
package io.prestosql.plugin.hive.util;

import com.google.common.cache.CacheLoader;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.Closeable;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestObjectPool
{
    @Test
    public void testObjectIsReused()
    {
        try (ObjectPool<Key, Value> pool = new ObjectPool<>(new ValueLoader(), 10, Duration.valueOf("1s"))) {
            Key key = new Key();
            Value value = getAndReturn(pool, key);
            assertEquals(getAndReturn(pool, key), value);
        }
    }

    @Test
    public void testObjectIsRecreatedWhenNotUsedForLongTime()
            throws Exception
    {
        try (ObjectPool<Key, Value> pool = new ObjectPool<>(new ValueLoader(), 10, Duration.valueOf("1ms"))) {
            Key key = new Key();
            Value value = getAndReturn(pool, key);
            sleep(10);

            assertNotEquals(getAndReturn(pool, key), value);
            assertTrue(value.isClosed());
        }
    }

    @Test
    public void testObjectIsRecreatedWhenInvalidated()
    {
        try (ObjectPool<Key, Value> pool = new ObjectPool<>(new ValueLoader(), 10, Duration.valueOf("1s"))) {
            Key key = new Key();
            Value value;
            try (ObjectPool.Lease<Value> lease = pool.get(key)) {
                value = lease.get();
                lease.invalidate();
            }
            assertNotEquals(getAndReturn(pool, key), value);
            assertTrue(value.isClosed());
        }
    }

    @Test
    public void testNewObjectEachTime()
    {
        Value value1;
        Value value2;
        Value value3;
        try (ObjectPool<Key, Value> pool = new ObjectPool<>(new ValueLoader(), 10, Duration.valueOf("1ms"))) {
            Key key = new Key();
            try (ObjectPool.Lease<Value> lease1 = pool.get(key)) {
                value1 = lease1.get();
                try (ObjectPool.Lease<Value> lease2 = pool.get(key)) {
                    value2 = lease2.get();
                    assertFalse(value2.isClosed());
                    try (ObjectPool.Lease<Value> lease3 = pool.get(key)) {
                        value3 = lease3.get();
                        assertFalse(value3.isClosed());

                        assertNotEquals(value1, value2);
                        assertNotEquals(value1, value3);
                        assertNotEquals(value2, value3);
                    }
                }
            }
        }
        assertTrue(value1.isClosed());
        assertTrue(value2.isClosed());
        assertTrue(value3.isClosed());
    }

    @Test
    public void testReturnObjectToClosedPool()
    {
        ObjectPool.Lease<Value> lease;
        try (ObjectPool<Key, Value> pool = new ObjectPool<>(new ValueLoader(), 10, Duration.valueOf("1s"))) {
            Key key = new Key();
            lease = pool.get(key);
            assertFalse(lease.get().isClosed());
        }
        assertFalse(lease.get().isClosed());
        lease.close();
        assertTrue(lease.get().isClosed());
    }

    private Value getAndReturn(ObjectPool<Key, Value> pool, Key key)
    {
        try (ObjectPool.Lease<Value> lease = pool.get(key)) {
            assertFalse(lease.get().isClosed());
            return lease.get();
        }
    }

    private static class Key {}

    private static class Value
            implements Closeable
    {
        private volatile boolean closed;

        public boolean isClosed()
        {
            return closed;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class ValueLoader
            extends CacheLoader<Key, Value>
    {
        @Override
        public Value load(Key key)
        {
            return new Value();
        }
    }
}
