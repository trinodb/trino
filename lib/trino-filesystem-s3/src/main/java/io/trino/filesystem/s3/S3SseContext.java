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
package io.trino.filesystem.s3;

import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record S3SseContext(S3SseType sseType, String sseKmsKeyId)
{
    public S3SseContext
    {
        requireNonNull(sseType, "sseType is null");
        checkArgument((sseType != S3SseType.KMS) || (sseKmsKeyId != null), "sseKmsKeyId is null for SSE-KMS");
    }

    public static S3SseContext withKmsKeyId(String kmsKeyId)
    {
        return new S3SseContext(S3SseType.KMS, kmsKeyId);
    }
}
