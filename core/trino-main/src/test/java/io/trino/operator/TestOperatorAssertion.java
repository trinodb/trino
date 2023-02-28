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
package io.trino.operator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.spi.Page;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOperatorAssertion
{
    private ScheduledExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newSingleThreadScheduledExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testToPagesWithBlockedOperator()
    {
        Operator operator = new BlockedOperator(new Duration(15, MILLISECONDS));
        List<Page> pages = OperatorAssertion.toPages(operator, emptyIterator());
        assertThat(pages).isEmpty();
    }

    private class BlockedOperator
            implements Operator
    {
        private final Duration unblockAfter;
        private final OperatorContext operatorContext;

        private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

        public BlockedOperator(Duration unblockAfter)
        {
            this.unblockAfter = requireNonNull(unblockAfter, "unblockAfter is null");
            this.operatorContext = TestingOperatorContext.create(executor);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return isBlocked;
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void finish()
        {
            if (this.isBlocked == NOT_BLOCKED) {
                this.isBlocked = Futures.scheduleAsync(Futures::immediateVoidFuture, unblockAfter.toMillis(), MILLISECONDS, executor);
            }
        }

        @Override
        public boolean isFinished()
        {
            return isBlocked != NOT_BLOCKED // finish() not called yet
                    && isBlocked.isDone();
        }

        @Override
        public Page getOutput()
        {
            return null;
        }
    }
}
