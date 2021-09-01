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
package io.trino.plugin.pulsar;

import java.util.Objects;

public class PulsarColumnMetadata
{
    public static final String KEY_SCHEMA_COLUMN_PREFIX = "__key.";
    public static final String PROPERTY_KEY_INTERNAL = "isInternal";
    public static final String PROPERTY_KEY_NAME_CASE_SENSITIVE = "nameWithCase";
    public static final String PROPERTY_KEY_HANDLE_TYPE = "handleType";
    public static final String PROPERTY_KEY_MAPPING = "mapping";
    public static final String PROPERTY_KEY_DATA_FORMAT = "dataFormat";
    public static final String PROPERTY_KEY_FORMAT_HINT = "hormatHint";

    private PulsarColumnMetadata()
    { }

    public static String getColumnName(PulsarColumnHandle.HandleKeyValueType handleKeyValueType, String name)
    {
        if (Objects.equals(PulsarColumnHandle.HandleKeyValueType.KEY, handleKeyValueType)) {
            return KEY_SCHEMA_COLUMN_PREFIX + name;
        }
        return name;
    }
}
