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
package io.trino.hive.formats.esri;

public enum OGCType {
    UNKNOWN((byte) 0),
    ST_POINT((byte) 1),
    ST_LINESTRING((byte) 2),
    ST_POLYGON((byte) 3),
    ST_MULTIPOINT((byte) 4),
    ST_MULTILINESTRING((byte) 5),
    ST_MULTIPOLYGON((byte) 6);

    private final byte index;

    OGCType(byte index)
    {
        this.index = index;
    }

    public byte getIndex()
    {
        return this.index;
    }
}
