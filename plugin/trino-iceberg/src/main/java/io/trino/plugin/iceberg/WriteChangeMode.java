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
package io.trino.plugin.iceberg;

public enum WriteChangeMode
{
    MOR("merge-on-read"),
    COW("copy-on-write");

    private final String icebergValue;

    WriteChangeMode(String icebergValue)
    {
        this.icebergValue = icebergValue;
    }

    public String toIcebergString()
    {
        return icebergValue;
    }

    public WriteChangeMode alternate()
    {
        return this == MOR ? COW : MOR;
    }

    public static WriteChangeMode fromIcebergString(String value)
    {
        return switch (value) {
            case "merge-on-read" -> MOR;
            case "copy-on-write" -> COW;
            default -> throw new IllegalArgumentException("Unexpected value: " + value);
        };
    }
}
