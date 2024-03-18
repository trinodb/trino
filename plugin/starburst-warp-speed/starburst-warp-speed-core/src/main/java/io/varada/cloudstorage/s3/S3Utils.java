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
package io.varada.cloudstorage.s3;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

import java.io.FileNotFoundException;
import java.io.IOException;

final class S3Utils
{
    private S3Utils()
    {
    }

    public static IOException handleAwsException(Throwable throwable, String action, S3Location location)
            throws IOException
    {
        Throwable initialThrowable = throwable;
        AwsServiceException exception = null;
        while (throwable != null) {
            if (throwable instanceof AwsServiceException) {
                exception = (AwsServiceException) throwable;
            }
            throwable = throwable.getCause();
        }
        if (exception != null) {
            if (exception.statusCode() == 404) {
                throw withCause(new FileNotFoundException(location.toString()), exception);
            }
            throw new IOException("AWS service error %s file: %s".formatted(action, location), exception);
        }
        throw new IOException("Error %s file: %s".formatted(action, location), initialThrowable);
    }

    private static <T extends Throwable> T withCause(T throwable, Throwable cause)
    {
        throwable.initCause(cause);
        return throwable;
    }
}
