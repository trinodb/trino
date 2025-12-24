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
package io.trino.util.variant;

import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Metadata;

import static io.trino.util.variant.PrimitiveMapVariantWriter.getMapKeys;

public record MapVariantWriter(MapType type, VariantWriter valueWriter)
        implements VariantWriter
{
    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        if (value == null) {
            return NullPlannedValue.NULL_PLANNED_VALUE;
        }

        SqlMap sqlMap = (SqlMap) value;
        int[] fieldIds = metadataBuilder.addFieldNames(getMapKeys(sqlMap));

        PlannedValue[] plannedValues = new PlannedValue[sqlMap.getSize()];

        Type valueType = type.getValueType();
        Block valueBlock = sqlMap.getRawValueBlock();
        int valueBlockOffset = sqlMap.getRawOffset();
        for (int entry = 0; entry < sqlMap.getSize(); entry++) {
            plannedValues[entry] = valueWriter.plan(metadataBuilder, valueType.getObject(valueBlock, valueBlockOffset + entry));
        }

        return new PlannedObjectValue(fieldIds, plannedValues);
    }
}
