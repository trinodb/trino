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

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringArrayKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

public final class TrinoAttributes
{
    private TrinoAttributes() {}

    public static final AttributeKey<String> QUERY_ID = stringKey("trino.query_id");
    public static final AttributeKey<String> STAGE_ID = stringKey("trino.stage_id");
    public static final AttributeKey<String> TASK_ID = stringKey("trino.task_id");
    public static final AttributeKey<String> PIPELINE_ID = stringKey("trino.pipeline_id");
    public static final AttributeKey<String> SPLIT_ID = stringKey("trino.split_id");

    public static final AttributeKey<String> QUERY_TYPE = stringKey("trino.query_type");

    public static final AttributeKey<Long> ERROR_CODE = longKey("trino.error_code");
    public static final AttributeKey<String> ERROR_NAME = stringKey("trino.error_name");
    public static final AttributeKey<String> ERROR_TYPE = stringKey("trino.error_type");

    public static final AttributeKey<String> CATALOG = stringKey("trino.catalog");
    public static final AttributeKey<String> SCHEMA = stringKey("trino.schema");
    public static final AttributeKey<String> TABLE = stringKey("trino.table");
    public static final AttributeKey<String> PROCEDURE = stringKey("trino.procedure");
    public static final AttributeKey<String> FUNCTION = stringKey("trino.function");
    public static final AttributeKey<String> HANDLE = stringKey("trino.handle");
    public static final AttributeKey<Boolean> CASCADE = booleanKey("trino.cascade");

    public static final AttributeKey<String> OPTIMIZER_NAME = stringKey("trino.optimizer");
    public static final AttributeKey<List<String>> OPTIMIZER_RULES = stringArrayKey("trino.optimizer.rules");

    public static final AttributeKey<Long> SPLIT_BATCH_MAX_SIZE = longKey("trino.split_batch.max_size");
    public static final AttributeKey<Long> SPLIT_BATCH_RESULT_SIZE = longKey("trino.split_batch.result_size");

    public static final AttributeKey<Long> SPLIT_SCHEDULED_TIME_NANOS = longKey("trino.split.scheduled_time_nanos");
    public static final AttributeKey<Long> SPLIT_CPU_TIME_NANOS = longKey("trino.split.cpu_time_nanos");
    public static final AttributeKey<Long> SPLIT_WAIT_TIME_NANOS = longKey("trino.split.wait_time_nanos");
    public static final AttributeKey<Long> SPLIT_START_TIME_NANOS = longKey("trino.split.start_time_nanos");
    public static final AttributeKey<Long> SPLIT_BLOCK_TIME_NANOS = longKey("trino.split.block_time_nanos");
    public static final AttributeKey<Boolean> SPLIT_BLOCKED = booleanKey("trino.split.blocked");

    public static final AttributeKey<String> EVENT_STATE = stringKey("state");
}
