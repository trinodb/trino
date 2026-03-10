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

import io.airlift.http.client.HeaderName;

public final class InternalHeaders
{
    public static final String TRINO_CURRENT_VERSION = "X-Trino-Current-Version";
    public static final HeaderName TRINO_CURRENT_VERSION_HEADER = HeaderName.of(TRINO_CURRENT_VERSION);

    public static final String TRINO_MAX_WAIT = "X-Trino-Max-Wait";
    public static final HeaderName TRINO_MAX_WAIT_HEADER = HeaderName.of(TRINO_MAX_WAIT);

    public static final String TRINO_MAX_SIZE = "X-Trino-Max-Size";
    public static final HeaderName TRINO_MAX_SIZE_HEADER = HeaderName.of(TRINO_MAX_SIZE);

    public static final String TRINO_TASK_INSTANCE_ID = "X-Trino-Task-Instance-Id";
    public static final HeaderName TRINO_TASK_INSTANCE_ID_HEADER = HeaderName.of(TRINO_TASK_INSTANCE_ID);

    public static final String TRINO_PAGE_TOKEN = "X-Trino-Page-Sequence-Id";
    public static final HeaderName TRINO_PAGE_TOKEN_HEADER = HeaderName.of(TRINO_PAGE_TOKEN);

    public static final String TRINO_PAGE_NEXT_TOKEN = "X-Trino-Page-End-Sequence-Id";
    public static final HeaderName TRINO_PAGE_NEXT_TOKEN_HEADER = HeaderName.of(TRINO_PAGE_NEXT_TOKEN);

    public static final String TRINO_BUFFER_COMPLETE = "X-Trino-Buffer-Complete";
    public static final HeaderName TRINO_BUFFER_COMPLETE_HEADER = HeaderName.of(TRINO_BUFFER_COMPLETE);

    public static final String TRINO_TASK_FAILED = "X-Trino-Task-Failed";
    public static final HeaderName TRINO_TASK_FAILED_HEADER = HeaderName.of(TRINO_TASK_FAILED);

    private InternalHeaders() {}
}
