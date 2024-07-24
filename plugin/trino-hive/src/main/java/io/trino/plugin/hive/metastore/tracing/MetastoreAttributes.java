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
package io.trino.plugin.hive.metastore.tracing;

import io.opentelemetry.api.common.AttributeKey;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

final class MetastoreAttributes
{
    private MetastoreAttributes() {}

    public static final AttributeKey<String> SCHEMA = stringKey("trino.schema");
    public static final AttributeKey<Long> SCHEMA_RESPONSE_COUNT = longKey("trino.hive.response.schema_count");
    public static final AttributeKey<String> TABLE = stringKey("trino.table");
    public static final AttributeKey<Long> TABLE_RESPONSE_COUNT = longKey("trino.hive.response.table_count");
    public static final AttributeKey<String> PARTITION = stringKey("trino.partition");
    public static final AttributeKey<String> FUNCTION = stringKey("trino.function");
    public static final AttributeKey<Long> FUNCTION_RESPONSE_COUNT = longKey("trino.hive.response.function_count");
    public static final AttributeKey<String> ACID_TRANSACTION = stringKey("trino.hive.acid_transaction");
    public static final AttributeKey<Long> PARTITION_REQUEST_COUNT = longKey("trino.hive.request.partition_count");
    public static final AttributeKey<Long> PARTITION_RESPONSE_COUNT = longKey("trino.hive.response.partition_count");
}
