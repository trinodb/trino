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
package io.trino.operator.scalar;

import io.trino.operator.scalar.MapSubscriptOperator.MissingKeyExceptionFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.SelectedPositions;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import static io.trino.operator.scalar.ArrayElementBlockProjection.selectElements;
import static io.trino.spi.type.TypeUtils.readNativeValue;

final class MapElementBlockProjection
{
    private MapElementBlockProjection() {}

    /// The values are selected through a dictionary of all the values of the maps, which measured slower than the
    /// generated projection for fixed width values, because they are cheap to copy.
    public static boolean isSupported(Type valueType)
    {
        return !(valueType instanceof FixedWidthType);
    }

    /// Selects the value of the key in each selected map. A missing key selects a null when the missing key exception
    /// factory is null, and fails otherwise.
    public static Block project(
            ConnectorSession session,
            SelectedPositions selectedPositions,
            Block mapColumn,
            Block keys,
            Type keyType,
            Type valueType,
            @Nullable MissingKeyExceptionFactory missingKeyExceptionFactory)
    {
        // Read the maps under a dictionary or a run, so only the selected maps are resolved
        MapBlock maps = (MapBlock) mapColumn.getUnderlyingValueBlock();
        int[] ids = new int[selectedPositions.size()];
        int missingValueCount = 0;

        // Read a constant key once, so only the maps are read at each position
        Object constantKey = null;
        Block keyColumn = keys;
        if (keys instanceof RunLengthEncodedBlock run && !run.isNull(0)) {
            constantKey = readNativeValue(keyType, run, 0);
            keyColumn = null;
        }

        Class<?> javaType = keyType.getJavaType();
        for (int selectedIndex = 0; selectedIndex < ids.length; selectedIndex++) {
            int position = selectedPositions.position(selectedIndex);
            int mapPosition = mapColumn.getUnderlyingValuePosition(position);
            if (maps.isNull(mapPosition) || (keyColumn != null && keyColumn.isNull(position))) {
                ids[selectedIndex] = -1;
                missingValueCount++;
                continue;
            }

            SqlMap map = maps.getMap(mapPosition);
            Object key = keyColumn == null ? constantKey : readNativeValue(keyType, keyColumn, position);
            int index;
            if (javaType == long.class) {
                index = map.seekKeyExact((long) key);
            }
            else if (javaType == double.class) {
                index = map.seekKeyExact((double) key);
            }
            else if (javaType == boolean.class) {
                index = map.seekKeyExact((boolean) key);
            }
            else {
                index = map.seekKeyExact(key);
            }

            if (index < 0) {
                if (missingKeyExceptionFactory != null) {
                    throw missingKeyExceptionFactory.create(session, key);
                }
                ids[selectedIndex] = -1;
                missingValueCount++;
            }
            else {
                ids[selectedIndex] = map.getRawOffset() + index;
            }
        }

        if (missingValueCount == ids.length) {
            return RunLengthEncodedBlock.create(valueType, null, ids.length);
        }
        return selectElements(maps.getRawValueBlock(), ids, missingValueCount);
    }
}
