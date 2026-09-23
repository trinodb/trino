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
package io.trino.operator.scalar.preimage;

import io.airlift.slice.Slice;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.airlift.slice.SliceUtf8.countCodePoints;

public final class VarcharToCharPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        return CastPreimages.compute(() -> project(context, resultDomain));
    }

    private static Optional<PreimageResult> project(Context context, Domain resultDomain)
    {
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        if (source instanceof VarcharType varcharType && target instanceof CharType charType) {
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > charType.getLength()) {
                return Optional.empty();
            }
            return CastPreimages.equality(resultDomain, source, value -> {
                int length = countCodePoints((Slice) value);
                if (length < varcharType.getBoundedLength()) {
                    return Optional.empty();
                }
                return Optional.of(length == varcharType.getBoundedLength() ? ValueSet.of(source, value) : ValueSet.none(source));
            });
        }
        return Optional.empty();
    }
}
