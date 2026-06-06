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
package io.trino.spi.function;

public record SchemaFunctionName(String schemaName, String functionName)
{
    public SchemaFunctionName
    {
        // FIXME: Now, in order to ensure the uniqueness of schemas within a catalogue,
        //        schemas are no longer converted to lowercase.
        if (schemaName.isEmpty()) {
            throw new IllegalArgumentException("schemaName is empty");
        }
        // FIXME: The function name is no longer converted to lowercase and is now treated as a table name.
        if (functionName.isEmpty()) {
            throw new IllegalArgumentException("functionName is empty");
        }
    }

    @Override
    public String toString()
    {
        return schemaName + '.' + functionName;
    }
}
