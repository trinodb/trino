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
package io.trino.plugin.varada.storage.read.predicates;

import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.predicate.Domain;

import java.nio.ByteBuffer;

public class AllPredicateFiller
        extends PredicateFiller<Domain>
{
    public AllPredicateFiller(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator);
    }

    @Override
    public void fillPredicate(Domain value, ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        writePredicateInfoToBuffer(predicateBuffer, predicateData);
    }

    @Override
    public void convertValues(Domain value, ByteBuffer predicateBuffer)
    {
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_ALL;
    }
}
