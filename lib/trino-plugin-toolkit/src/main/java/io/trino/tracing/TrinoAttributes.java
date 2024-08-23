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
package io.trino.tracing;

import io.opentelemetry.api.common.AttributeKey;

import java.util.List;

public final class TrinoAttributes
{
    private TrinoAttributes() {}

    public static final AttributeKey<String> QUERY_ID = AttributeKey.stringKey("trino.query_id");
    public static final AttributeKey<String> STAGE_ID = AttributeKey.stringKey("trino.stage_id");
    public static final AttributeKey<String> TASK_ID = AttributeKey.stringKey("trino.task_id");
    public static final AttributeKey<String> PIPELINE_ID = AttributeKey.stringKey("trino.pipeline_id");
    public static final AttributeKey<String> SPLIT_ID = AttributeKey.stringKey("trino.split_id");

    public static final AttributeKey<String> QUERY_TYPE = AttributeKey.stringKey("trino.query_type");

    public static final AttributeKey<Long> ERROR_CODE = AttributeKey.longKey("trino.error_code");
    public static final AttributeKey<String> ERROR_NAME = AttributeKey.stringKey("trino.error_name");
    public static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("trino.error_type");

    public static final AttributeKey<String> CATALOG = AttributeKey.stringKey("trino.catalog");
    public static final AttributeKey<String> SCHEMA = AttributeKey.stringKey("trino.schema");
    public static final AttributeKey<String> TABLE = AttributeKey.stringKey("trino.table");
    public static final AttributeKey<String> PROCEDURE = AttributeKey.stringKey("trino.procedure");
    public static final AttributeKey<String> FUNCTION = AttributeKey.stringKey("trino.function");
    public static final AttributeKey<String> HANDLE = AttributeKey.stringKey("trino.handle");
    public static final AttributeKey<Boolean> CASCADE = AttributeKey.booleanKey("trino.cascade");

    public static final AttributeKey<String> OPTIMIZER_NAME = AttributeKey.stringKey("trino.optimizer");
    public static final AttributeKey<List<String>> OPTIMIZER_RULES = AttributeKey.stringArrayKey("trino.optimizer.rules");

    public static final AttributeKey<Long> SPLIT_BATCH_MAX_SIZE = AttributeKey.longKey("trino.split_batch.max_size");
    public static final AttributeKey<Long> SPLIT_BATCH_RESULT_SIZE = AttributeKey.longKey("trino.split_batch.result_size");

    public static final AttributeKey<Long> SPLIT_SCHEDULED_TIME_NANOS = AttributeKey.longKey("trino.split.scheduled_time_nanos");
    public static final AttributeKey<Long> SPLIT_CPU_TIME_NANOS = AttributeKey.longKey("trino.split.cpu_time_nanos");
    public static final AttributeKey<Long> SPLIT_WAIT_TIME_NANOS = AttributeKey.longKey("trino.split.wait_time_nanos");
    public static final AttributeKey<Long> SPLIT_START_TIME_NANOS = AttributeKey.longKey("trino.split.start_time_nanos");
    public static final AttributeKey<Long> SPLIT_BLOCK_TIME_NANOS = AttributeKey.longKey("trino.split.block_time_nanos");
    public static final AttributeKey<Boolean> SPLIT_BLOCKED = AttributeKey.booleanKey("trino.split.blocked");

    public static final AttributeKey<String> EVENT_STATE = AttributeKey.stringKey("state");

    public static final AttributeKey<String> FAILURE_MESSAGE = AttributeKey.stringKey("failure");
    public static final AttributeKey<String> PRIVILEGE_GRANT = AttributeKey.stringKey("grant");
    public static final AttributeKey<String> ENTITY = AttributeKey.stringKey("entity");
}
