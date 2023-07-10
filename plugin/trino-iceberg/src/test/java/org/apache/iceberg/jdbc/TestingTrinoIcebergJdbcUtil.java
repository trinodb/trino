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

package org.apache.iceberg.jdbc;

public final class TestingTrinoIcebergJdbcUtil
{
    public static final String CREATE_CATALOG_TABLE = JdbcUtil.CREATE_CATALOG_TABLE;
    public static final String CREATE_NAMESPACE_PROPERTIES_TABLE = JdbcUtil.CREATE_NAMESPACE_PROPERTIES_TABLE;

    private TestingTrinoIcebergJdbcUtil() {}
}
