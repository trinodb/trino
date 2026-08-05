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

import io.trino.filesystem.TrinoFileSystemException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;

import java.io.IOException;

final class S3Exceptions
{
    private S3Exceptions() {}

    /**
     * Translates an AWS SDK exception into an {@link IOException}, classifying it at throw time:
     * fatal errors (for example a {@code 403 Forbidden}) become a {@link TrinoFileSystemException}
     * so that callers stop retrying, while transient errors (throttling, {@code 5xx}, client side IO
     * errors) surface as a plain {@link IOException} so that callers may retry.
     */
    static IOException handleS3Exception(SdkException exception, String message)
    {
        if (isUnrecoverable(exception)) {
            return new TrinoFileSystemException(message, exception);
        }
        return new IOException(message, exception);
    }

    private static boolean isUnrecoverable(SdkException exception)
    {
        if (exception instanceof SdkServiceException serviceException) {
            return !serviceException.isThrottlingException()
                    && !serviceException.isClockSkewException()
                    && isUnrecoverableStatusCode(serviceException.statusCode());
        }
        // Client-side errors (IO errors, connection resets, ...) are transient and worth retrying.
        return false;
    }

    private static boolean isUnrecoverableStatusCode(int statusCode)
    {
        return switch (statusCode) {
            case 408, 429, 500, 502, 503, 504 -> false;
            default -> true;
        };
    }
}
