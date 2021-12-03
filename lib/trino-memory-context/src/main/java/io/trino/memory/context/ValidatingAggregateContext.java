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
package io.trino.memory.context;

import static java.util.Objects.requireNonNull;

public class ValidatingAggregateContext
        implements AggregatedMemoryContext
{
    private final AggregatedMemoryContext delegate;
    private final MemoryAllocationValidator memoryValidator;

    public ValidatingAggregateContext(AggregatedMemoryContext delegate, MemoryAllocationValidator memoryValidator)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.memoryValidator = requireNonNull(memoryValidator, "memoryValidator is null");
    }

    @Override
    public AggregatedMemoryContext newAggregatedMemoryContext()
    {
        return new ValidatingAggregateContext(delegate.newAggregatedMemoryContext(), memoryValidator);
    }

    @Override
    public LocalMemoryContext newLocalMemoryContext(String allocationTag)
    {
        return new ValidatingLocalMemoryContext(delegate.newLocalMemoryContext(allocationTag), allocationTag, memoryValidator);
    }

    @Override
    public long getBytes()
    {
        return delegate.getBytes();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
