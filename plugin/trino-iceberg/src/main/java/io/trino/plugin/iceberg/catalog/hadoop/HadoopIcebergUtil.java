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
package io.trino.plugin.iceberg.catalog.hadoop;

public class HadoopIcebergUtil
{
    private HadoopIcebergUtil() {}

    public static boolean isHadoopSystemSchema(String schemaName)
    {
        if ("information_schema".equals(schemaName)) {
            // For things like listing columns in information_schema.columns table, we need to explicitly filter out Hadoop's own information_schema.
            // TODO https://github.com/trinodb/trino/issues/1559 this should be filtered out in engine.
            return true;
        }
        if ("system".equals(schemaName)) {
            // Hadoop `system` schema contains no objects we can handle, so there is no point in exposing it.
            // Also, exposing it may require proper handling in access control.
            return true;
        }
        return false;
    }
}
