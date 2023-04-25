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
package io.trino.operator.join.smj;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.spi.Page;

public class SortMergeJoinBuildOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final PageIterator buildPages;
    private SettableFuture<Void> full;
    boolean finish;

    public SortMergeJoinBuildOperator(OperatorContext operatorContext, PageIterator buildPages)
    {
        this.operatorContext = operatorContext;
        this.buildPages = buildPages;
        this.full = this.buildPages.getFull();
        this.finish = false;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return full.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        if (buildPages.isClosed()) {
            finish = true;
        }
        else {
            full = buildPages.add(page);
        }
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finish()
    {
        finish = true;
        buildPages.noMorePage();
    }

    @Override
    public boolean isFinished()
    {
        return finish;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return full;
    }
}
