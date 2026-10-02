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

import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.type.DateTimes.scaleFactor;

public final class TimeCastPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        TimeType source = (TimeType) context.signature().getArgumentTypes().getFirst();
        if (!context.functions().canCoerce(source, resultDomain.getType())) {
            return Optional.empty();
        }
        // TimeType's advertised upper bound is midnight of the following day.
        // Use the final representable time so reverse rounding cannot wrap to zero.
        Type.Range range = new Type.Range(0L, 86_400_000_000_000_000L - scaleFactor(source.getPrecision(), 12));
        return OrderPreservingCastPreimage.project(context, resultDomain, Optional.of(range));
    }
}
