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
package io.trino.testing.containers.environment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class ProductTestExtensionFixtures
{
    private ProductTestExtensionFixtures() {}

    static final class FakeEnvironment
            extends ProductTestEnvironment
    {
        static final AtomicInteger startCount = new AtomicInteger();
        static final AtomicInteger closeCount = new AtomicInteger();
        static final AtomicInteger beforeEachCount = new AtomicInteger();
        static final AtomicInteger afterEachCount = new AtomicInteger();
        private volatile boolean running;

        static void reset()
        {
            startCount.set(0);
            closeCount.set(0);
            beforeEachCount.set(0);
            afterEachCount.set(0);
        }

        @Override
        public void start()
        {
            if (running) {
                return;
            }
            startCount.incrementAndGet();
            running = true;
        }

        @Override
        public Connection createTrinoConnection()
                throws SQLException
        {
            throw new UnsupportedOperationException("Fake environment");
        }

        @Override
        public Connection createTrinoConnection(String user)
                throws SQLException
        {
            throw new UnsupportedOperationException("Fake environment");
        }

        @Override
        public String getTrinoJdbcUrl()
        {
            return "jdbc:fake://localhost/test";
        }

        @Override
        public boolean isRunning()
        {
            return running;
        }

        @Override
        protected void beforeEachTest()
        {
            beforeEachCount.incrementAndGet();
        }

        @Override
        protected void afterEachTest()
        {
            afterEachCount.incrementAndGet();
        }

        @Override
        protected void doClose()
        {
            closeCount.incrementAndGet();
            running = false;
        }
    }

    @ProductTest
    @RequiresEnvironment(FakeEnvironment.class)
    static final class EnvironmentReuseClassA
    {
        @Test
        void testOne(ProductTestEnvironment env)
        {
            assertThat(env).isInstanceOf(FakeEnvironment.class);
            assertThat(env.isRunning()).isTrue();
        }

        @Test
        void testTwo(ProductTestEnvironment env)
        {
            assertThat(env).isInstanceOf(FakeEnvironment.class);
            assertThat(env.isRunning()).isTrue();
        }
    }

    @ProductTest
    @RequiresEnvironment(FakeEnvironment.class)
    static final class EnvironmentReuseClassB
    {
        @Test
        void testThree(ProductTestEnvironment env)
        {
            assertThat(env).isInstanceOf(FakeEnvironment.class);
            assertThat(env.isRunning()).isTrue();
        }

        @Test
        void testFour(ProductTestEnvironment env)
        {
            assertThat(env).isInstanceOf(FakeEnvironment.class);
            assertThat(env.isRunning()).isTrue();
        }
    }

    static class UnrelatedEnvironment
            extends ProductTestEnvironment
    {
        @Override
        public void start() {}

        @Override
        public Connection createTrinoConnection()
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection createTrinoConnection(String user)
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getTrinoJdbcUrl()
        {
            return "jdbc:fake://localhost/unrelated";
        }

        @Override
        public boolean isRunning()
        {
            return true;
        }

        @Override
        protected void doClose() {}
    }

    static final class FakeSubEnvironment
            extends FakeBaseEnvironment
    {
    }

    static class FakeBaseEnvironment
            extends ProductTestEnvironment
    {
        @Override
        public void start() {}

        @Override
        public Connection createTrinoConnection()
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection createTrinoConnection(String user)
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getTrinoJdbcUrl()
        {
            return "jdbc:fake://localhost/base";
        }

        @Override
        public boolean isRunning()
        {
            return true;
        }

        @Override
        protected void doClose() {}
    }

    static final class HookOrderEnvironment
            extends ProductTestEnvironment
    {
        static final StringBuilder events = new StringBuilder();

        static void reset()
        {
            events.setLength(0);
        }

        @Override
        public void start() {}

        @Override
        public Connection createTrinoConnection()
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection createTrinoConnection(String user)
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getTrinoJdbcUrl()
        {
            return "jdbc:fake://localhost/hook-order";
        }

        @Override
        public boolean isRunning()
        {
            return true;
        }

        @Override
        protected void beforeEachTest()
        {
            events.append("env-before>");
        }

        @Override
        protected void afterEachTest()
        {
            events.append("env-after>");
        }

        @Override
        protected void doClose() {}
    }

    static final class CleanupFailingEnvironment
            extends ProductTestEnvironment
    {
        static volatile boolean cleanupFailureEnabled;

        static void reset()
        {
            cleanupFailureEnabled = false;
        }

        @Override
        public void start() {}

        @Override
        public Connection createTrinoConnection()
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection createTrinoConnection(String user)
                throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getTrinoJdbcUrl()
        {
            return "jdbc:fake://localhost/cleanup-failure";
        }

        @Override
        public boolean isRunning()
        {
            return true;
        }

        @Override
        protected void afterEachTest()
        {
            if (cleanupFailureEnabled) {
                throw new RuntimeException("cleanup failure");
            }
        }

        @Override
        protected void doClose() {}
    }

    abstract static class AbstractFakeEnvironmentTest
    {
        @Test
        void testInherited(FakeEnvironment env)
        {
            assertThat(env).isNotNull();
            assertThat(env.isRunning()).isTrue();
        }
    }

    abstract static class AbstractBaseEnvironmentTest
    {
        @Test
        void testInherited(FakeBaseEnvironment env)
        {
            assertThat(env).isNotNull();
            assertThat(env.isRunning()).isTrue();
        }
    }

    @ProductTest
    @RequiresEnvironment(UnrelatedEnvironment.class)
    static final class MismatchedHierarchyCase
            extends AbstractFakeEnvironmentTest
    {
    }

    @ProductTest
    @RequiresEnvironment(FakeSubEnvironment.class)
    static final class ValidHierarchyCase
            extends AbstractBaseEnvironmentTest
    {
    }

    @ProductTest
    static final class ConcreteProductCaseWithoutEnvironment
    {
        @Test
        void testMissingEnvironmentAnnotation() {}
    }

    @ProductTest
    abstract static class AbstractProductTestWithoutEnvironment
    {
        @Test
        void testInheritedFromAbstractBase(FakeEnvironment env)
        {
            assertThat(env).isNotNull();
        }
    }

    @ProductTest
    @RequiresEnvironment(FakeEnvironment.class)
    static final class AbstractProductCaseSubclassWithEnvironment
            extends AbstractProductTestWithoutEnvironment
    {
    }

    @ProductTest
    @RequiresEnvironment(HookOrderEnvironment.class)
    static final class HookOrderingCase
    {
        @BeforeEach
        void beforeEach()
        {
            HookOrderEnvironment.events.append("test-before>");
        }

        @Test
        void testBody(HookOrderEnvironment environment)
        {
            assertThat(environment).isNotNull();
            HookOrderEnvironment.events.append("test>");
        }

        @AfterEach
        void afterEach()
        {
            HookOrderEnvironment.events.append("test-after>");
        }
    }

    @ProductTest
    @RequiresEnvironment(CleanupFailingEnvironment.class)
    static final class CleanupFailureWithPrimaryFailureCase
    {
        @Test
        void failingTest(CleanupFailingEnvironment environment)
        {
            assertThat(environment).isNotNull();
            throw new AssertionError("primary test failure");
        }
    }

    @ProductTest
    @RequiresEnvironment(CleanupFailingEnvironment.class)
    static final class CleanupFailureWithoutPrimaryFailureCase
    {
        @Test
        void passingTest(CleanupFailingEnvironment environment)
        {
            assertThat(environment).isNotNull();
        }
    }
}
