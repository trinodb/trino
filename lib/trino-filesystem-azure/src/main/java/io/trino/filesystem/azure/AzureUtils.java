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
package io.trino.filesystem.azure;

import com.azure.core.exception.AzureException;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.models.CustomerProvidedKey;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.OptionalInt;

final class AzureUtils
{
    private AzureUtils() {}

    public static IOException handleAzureException(Throwable exception, String action, AzureLocation location)
            throws IOException
    {
        if (isFileNotFoundException(exception)) {
            throw withCause(new FileNotFoundException(location.toString()), exception);
        }
        if (exception instanceof AzureException) {
            String message = "Azure service error %s file: %s".formatted(action, location);
            if (isRetryable(exception)) {
                throw new IOException(message, exception);
            }
            throw new TrinoFileSystemException(message, exception);
        }
        throw new IOException("Error %s file: %s".formatted(action, location), exception);
    }

    private static boolean isRetryable(Throwable exception)
    {
        OptionalInt statusCode = statusCode(exception);
        return statusCode.isPresent() && isRetryableStatusCode(statusCode.getAsInt());
    }

    private static boolean isRetryableStatusCode(int statusCode)
    {
        return switch (statusCode) {
            case 408, 429, 500, 502, 503, 504 -> true;
            default -> false;
        };
    }

    private static OptionalInt statusCode(Throwable exception)
    {
        if (exception instanceof BlobStorageException blobStorageException) {
            return OptionalInt.of(blobStorageException.getStatusCode());
        }
        if (exception instanceof DataLakeStorageException dataLakeStorageException) {
            return OptionalInt.of(dataLakeStorageException.getStatusCode());
        }
        return OptionalInt.empty();
    }

    public static boolean isFileNotFoundException(Throwable exception)
    {
        if (exception instanceof BlobStorageException blobStorageException) {
            return BlobErrorCode.BLOB_NOT_FOUND.equals(blobStorageException.getErrorCode());
        }
        if (exception instanceof DataLakeStorageException dataLakeStorageException) {
            return "PathNotFound".equals(dataLakeStorageException.getErrorCode());
        }
        return false;
    }

    private static <T extends Throwable> T withCause(T throwable, Throwable cause)
    {
        throwable.initCause(cause);
        return throwable;
    }

    public static CustomerProvidedKey lakeCustomerProvidedKey(EncryptionKey key)
    {
        return new CustomerProvidedKey(encodedKey(key));
    }

    public static com.azure.storage.blob.models.CustomerProvidedKey blobCustomerProvidedKey(EncryptionKey key)
    {
        return new com.azure.storage.blob.models.CustomerProvidedKey(encodedKey(key));
    }

    public static String encodedKey(EncryptionKey key)
    {
        return Base64.getEncoder().encodeToString(key.key());
    }

    public static String keySha256Checksum(EncryptionKey key)
    {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] hash = sha256.digest(key.key());
            return Base64.getEncoder().encodeToString(hash);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
