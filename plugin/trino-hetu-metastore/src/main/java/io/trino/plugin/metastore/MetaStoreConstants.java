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
package io.trino.plugin.metastore;

public class MetaStoreConstants
{
    private MetaStoreConstants() {}

    /**
     * MetaStore collection name six
     */
    public static final String HETU_META_STORE_CATALOGCACHE_NAME = "catalogCache";
    public static final String HETU_META_STORE_CATALOGSCACHE_NAME = "catalogsCache";
    public static final String HETU_META_STORE_DATABASECACHE_NAME = "databaseCache";
    public static final String HETU_META_STORE_DATABASESCACHE_NAME = "databasesCache";
    public static final String HETU_META_STORE_TABLECACHE_NAME = "tableCache";
    public static final String HETU_META_STORE_TABLESCACHE_NAME = "tablesCache";

    public static final String GLOBAL = "global";
    public static final String LOCAL = "local";
}
