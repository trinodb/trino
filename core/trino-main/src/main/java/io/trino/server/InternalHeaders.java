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
package io.trino.server;

public final class InternalHeaders
{
    public static final String TRINO_CURRENT_VERSION = "X-Trino-Current-Version";
    public static final String TRINO_MAX_WAIT = "X-Trino-Max-Wait";
    public static final String TRINO_MAX_SIZE = "X-Trino-Max-Size";
    public static final String TRINO_TASK_INSTANCE_ID = "X-Trino-Task-Instance-Id";
    public static final String TRINO_PAGE_TOKEN = "X-Trino-Page-Sequence-Id";
    public static final String TRINO_PAGE_NEXT_TOKEN = "X-Trino-Page-End-Sequence-Id";
    public static final String TRINO_BUFFER_COMPLETE = "X-Trino-Buffer-Complete";
    public static final String TRINO_TASK_FAILED = "X-Trino-Task-Failed";

    private InternalHeaders() {}
}
