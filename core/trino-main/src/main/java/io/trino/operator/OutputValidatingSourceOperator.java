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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.function.Supplier;

import static io.trino.operator.PageValidations.validateOutputPageTypes;
import static java.util.Objects.requireNonNull;

public class OutputValidatingSourceOperator
        implements SourceOperator
{
    private final SourceOperator delegate;
    private final List<Type> outputTypes;
    private final Supplier<String> debugContextSupplier;

    public OutputValidatingSourceOperator(SourceOperator delegate, List<Type> outputTypes, Supplier<String> debugContextSupplier)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.debugContextSupplier = requireNonNull(debugContextSupplier, "debugContextSupplier is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return delegate.getOperatorContext();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return delegate.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        delegate.addInput(page);
    }

    @Override
    public Page getOutput()
    {
        Page page = delegate.getOutput();
        if (page != null) {
            validateOutputPageTypes(page, outputTypes, debugContextSupplier);
        }
        return page;
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        return delegate.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke()
    {
        delegate.finishMemoryRevoke();
    }

    @Override
    public void finish()
    {
        delegate.finish();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        delegate.close();
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return delegate.getSourceId();
    }

    @Override
    public void addSplit(Split split)
    {
        delegate.addSplit(split);
    }

    @Override
    public void noMoreSplits()
    {
        delegate.noMoreSplits();
    }
}
