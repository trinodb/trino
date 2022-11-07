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
package io.trino.parquet.reader;

import io.trino.parquet.PrimitiveField;
import io.trino.spi.type.DecimalType;

public final class DecimalColumnReaderFactory
{
    private DecimalColumnReaderFactory() {}

    public static PrimitiveColumnReader createReader(PrimitiveField field, DecimalType parquetDecimalType)
    {
        if (parquetDecimalType.isShort()) {
            return new ShortDecimalColumnReader(field, parquetDecimalType);
        }
        return new LongDecimalColumnReader(field, parquetDecimalType);
    }
}
