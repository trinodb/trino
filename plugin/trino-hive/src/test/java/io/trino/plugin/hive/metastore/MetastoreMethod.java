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

public enum MetastoreMethod
{
    CREATE_DATABASE,
    DROP_DATABASE,
    CREATE_TABLE,
    GET_ALL_DATABASES,
    GET_DATABASE,
    GET_TABLE,
    GET_ALL_TABLES,
    GET_ALL_TABLES_FROM_DATABASE,
    GET_RELATION_TYPES_FROM_DATABASE,
    GET_ALL_RELATION_TYPES,
    GET_TABLES_WITH_PARAMETER,
    GET_TABLE_STATISTICS,
    GET_ALL_VIEWS,
    GET_ALL_VIEWS_FROM_DATABASE,
    UPDATE_TABLE_STATISTICS,
    ADD_PARTITIONS,
    GET_PARTITION_NAMES_BY_FILTER,
    GET_PARTITIONS_BY_NAMES,
    GET_PARTITION,
    GET_PARTITION_STATISTICS,
    UPDATE_PARTITION_STATISTICS,
    REPLACE_TABLE,
    DROP_TABLE,
}
