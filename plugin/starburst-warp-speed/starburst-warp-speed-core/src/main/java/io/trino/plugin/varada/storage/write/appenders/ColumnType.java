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
package io.trino.plugin.varada.storage.write.appenders;

public enum ColumnType
{
    LONG(0),
    LONG_DECIMAL(1),
    BOOLEAN(2),
    INTEGER(3),
    TinyInt(4),
    SMALL_INT(5),
    DOUBLE(6),
    VARCHAR(7),
    ARRAY(8),
    REAL(9),
    CRC_LONG(10),
    CRC_LONG_DECIMAL(11),
    CRC_BOOL(12),
    CRC_INTEGER(13),
    CRC_TinyInt(14),
    CRC_SMALL_INT(15),
    CRC_DOUBLE(16),
    CRC_VARCHAR(17),
    CRC_ARRAY(18),
    CRC_REAL(19);

    private final int columnType;

    ColumnType(int intValue)
    {
        this.columnType = intValue;
    }

    public int value()
    {
        return columnType;
    }
}
