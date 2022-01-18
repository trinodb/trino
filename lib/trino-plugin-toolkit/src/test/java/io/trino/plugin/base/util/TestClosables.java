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
package io.trino.plugin.base.util;

import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class TestClosables
{
    @Test
    public void testCloseAllSuppressNonThrowing()
    {
        RuntimeException rootException = new RuntimeException("root");
        TestClosable closable = new TestClosable(Optional.empty());
        closeAllSuppress(rootException, closable);
        assertTrue(closable.isClosed());
        assertThat(rootException.getSuppressed()).isEmpty();
    }

    @Test
    public void testCloseAllSuppressThrowingOther()
    {
        RuntimeException rootException = new RuntimeException("root");
        RuntimeException closeException = new RuntimeException("close");
        TestClosable closable = new TestClosable(Optional.of(closeException));
        closeAllSuppress(rootException, closable);
        assertTrue(closable.isClosed());
        assertThat(rootException.getSuppressed()).containsExactly(closeException);
    }

    @Test
    public void testCloseAllSuppressThrowingRoot()
    {
        RuntimeException rootException = new RuntimeException("root");
        TestClosable closable = new TestClosable(Optional.of(rootException));
        closeAllSuppress(rootException, closable);
        assertTrue(closable.isClosed());
        assertThat(rootException.getSuppressed()).isEmpty();
    }

    @Test
    public void testCloseAllSuppressNullClosable()
    {
        RuntimeException rootException = new RuntimeException("root");
        closeAllSuppress(rootException, (AutoCloseable) null);
        assertThat(rootException.getSuppressed()).isEmpty();
    }

    @Test
    public void testCloseAllSuppressMultipleClosables()
    {
        RuntimeException rootException = new RuntimeException("root");
        RuntimeException closeException1 = new RuntimeException("close");
        RuntimeException closeException2 = new RuntimeException("close2");
        TestClosable closable1 = new TestClosable(Optional.of(closeException1));
        TestClosable closable2 = new TestClosable(Optional.of(closeException2));
        TestClosable closable3 = new TestClosable(Optional.empty()); // non throwing
        TestClosable closable4 = new TestClosable(Optional.of(rootException)); // throwing root
        closeAllSuppress(rootException, closable1, closable2, closable3, closable4, null);
        assertTrue(closable1.isClosed());
        assertTrue(closable2.isClosed());
        assertTrue(closable3.isClosed());
        assertTrue(closable4.isClosed());
        assertThat(rootException.getSuppressed()).containsExactly(closeException1, closeException2);
    }

    private static class TestClosable
            implements AutoCloseable
    {
        private final Optional<Exception> closeException;
        private boolean closed;

        public TestClosable(Optional<Exception> closeException)
        {
            this.closeException = requireNonNull(closeException, "closeException is null");
        }

        @Override
        public void close()
                throws Exception
        {
            closed = true;
            if (closeException.isPresent()) {
                throw closeException.get();
            }
        }

        public boolean isClosed()
        {
            return closed;
        }
    }
}
