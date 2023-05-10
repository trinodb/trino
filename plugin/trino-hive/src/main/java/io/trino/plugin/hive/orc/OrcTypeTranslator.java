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
package io.trino.plugin.hive.orc;

import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToVarcharCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.ShortTimestampToVarcharCoercer;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.spi.type.TimestampType.createTimestampType;

public final class OrcTypeTranslator
{
    private OrcTypeTranslator() {}

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(OrcTypeKind fromOrcType, Type toTrinoType, HiveTimestampPrecision timestampPrecision)
    {
        if (fromOrcType.equals(OrcTypeKind.TIMESTAMP) && toTrinoType instanceof VarcharType varcharType) {
            TimestampType timestampType = createTimestampType(timestampPrecision.getPrecision());
            if (timestampType.isShort()) {
                return Optional.of(new ShortTimestampToVarcharCoercer(timestampType, varcharType));
            }
            return Optional.of(new LongTimestampToVarcharCoercer(timestampType, varcharType));
        }
        return Optional.empty();
    }
}
