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
package io.trino.plugin.hive.metastore;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

public enum MetastoreMethod
{
    CREATE_DATABASE,
    DROP_DATABASE,
    CREATE_TABLE,
    GET_ALL_DATABASES,
    GET_DATABASE,
    GET_TABLE,
    GET_TABLES,
    GET_TABLE_COLUMN_STATISTICS,
    UPDATE_TABLE_STATISTICS,
    ADD_PARTITIONS,
    GET_PARTITION_NAMES_BY_FILTER,
    GET_PARTITIONS_BY_NAMES,
    GET_PARTITION,
    GET_PARTITION_COLUMN_STATISTICS,
    UPDATE_PARTITION_STATISTICS,
    REPLACE_TABLE,
    DROP_TABLE,
    /**/;

    public static MetastoreMethod fromMethodName(String name)
    {
        return valueOf(LOWER_CAMEL.to(UPPER_UNDERSCORE, name));
    }
}
