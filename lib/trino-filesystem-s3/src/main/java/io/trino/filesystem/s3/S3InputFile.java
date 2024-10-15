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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.encryption.EncryptionKey;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static io.trino.filesystem.s3.S3SseCUtils.encoded;
import static io.trino.filesystem.s3.S3SseCUtils.md5Checksum;
import static java.util.Objects.requireNonNull;

final class S3InputFile
        implements TrinoInputFile
{
    private final S3Client client;
    private final S3Location location;
    private final S3Context context;
    private final RequestPayer requestPayer;
    private final Optional<EncryptionKey> key;
    private Long length;
    private Instant lastModified;

    public S3InputFile(S3Client client, S3Context context, S3Location location, Long length, Instant lastModified, Optional<EncryptionKey> key)
    {
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.context = requireNonNull(context, "context is null");
        this.requestPayer = context.requestPayer();
        this.length = length;
        this.lastModified = lastModified;
        this.key = requireNonNull(key, "key is null");
        location.location().verifyValidFileLocation();
    }

    @Override
    public TrinoInput newInput()
    {
        return new S3Input(location(), client, newGetObjectRequest());
    }

    @Override
    public TrinoInputStream newStream()
    {
        return new S3InputStream(location(), client, newGetObjectRequest(), length);
    }

    @Override
    public long length()
            throws IOException
    {
        if ((length == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if ((lastModified == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return lastModified;
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return headObject();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private GetObjectRequest newGetObjectRequest()
    {
        return GetObjectRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .applyMutation(builder -> key.ifPresent(encryption -> {
                    builder.sseCustomerKey(encoded(encryption));
                    builder.sseCustomerAlgorithm(encryption.algorithm());
                    builder.sseCustomerKeyMD5(md5Checksum(encryption));
                }))
                .build();
    }

    private boolean headObject()
            throws IOException
    {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .applyMutation(builder -> key.ifPresent(encryption -> {
                    builder.sseCustomerKey(encoded(encryption));
                    builder.sseCustomerAlgorithm(encryption.algorithm());
                    builder.sseCustomerKeyMD5(md5Checksum(encryption));
                }))
                .build();

        try {
            HeadObjectResponse response = client.headObject(request);
            if (length == null) {
                length = response.contentLength();
            }
            if (lastModified == null) {
                lastModified = response.lastModified();
            }
            return true;
        }
        catch (NoSuchKeyException e) {
            return false;
        }
        catch (SdkException e) {
            throw new TrinoFileSystemException("S3 HEAD request failed for file: " + location, e);
        }
    }
}
