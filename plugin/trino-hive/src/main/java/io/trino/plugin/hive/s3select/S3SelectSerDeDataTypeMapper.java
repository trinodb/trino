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
package io.trino.plugin.hive.s3select;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.util.HiveClassNames.JSON_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;

public class S3SelectSerDeDataTypeMapper
{
    // Contains mapping of SerDe class name -> data type. Multiple SerDe classes can be mapped to the same data type.
    private static final Map<String, S3SelectDataType> serDeToDataTypeMapping = Map.of(
            LAZY_SIMPLE_SERDE_CLASS, S3SelectDataType.CSV,
            JSON_SERDE_CLASS, S3SelectDataType.JSON);

    private S3SelectSerDeDataTypeMapper() {}

    public static Optional<S3SelectDataType> getDataType(String serdeName)
    {
        return Optional.ofNullable(serDeToDataTypeMapping.get(serdeName));
    }

    public static boolean doesSerDeExist(String serdeName)
    {
        return serDeToDataTypeMapping.containsKey(serdeName);
    }
}
