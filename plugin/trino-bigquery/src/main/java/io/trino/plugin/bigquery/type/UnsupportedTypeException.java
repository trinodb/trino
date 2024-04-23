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
package io.trino.plugin.bigquery.type;

import com.google.cloud.bigquery.StandardSQLTypeName;

/**
 * Unsupported type exception occurs only for data types
 * known to be not supported, like STRUCT, not all unknown tokens.
 */
public class UnsupportedTypeException
        extends IllegalArgumentException
{
    private final StandardSQLTypeName typeName;

    public UnsupportedTypeException(StandardSQLTypeName typeName, String errorMessage)
    {
        super(errorMessage);
        this.typeName = typeName;
    }

    public StandardSQLTypeName getTypeName()
    {
        return typeName;
    }
}
