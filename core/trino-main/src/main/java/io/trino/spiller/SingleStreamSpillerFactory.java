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
package io.trino.spiller;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.SpillContext;
import io.trino.spi.type.Type;

import java.util.List;

public interface SingleStreamSpillerFactory
{
    default SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        return create(types, spillContext, memoryContext, false);
    }

    SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext, boolean parallelSpill);

    static SingleStreamSpillerFactory unsupportedSingleStreamSpillerFactory()
    {
        return (types, spillContext, memoryContext, parallelSpill) -> {
            throw new UnsupportedOperationException();
        };
    }
}
