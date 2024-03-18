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

import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.varada.cloudstorage.CloudStorageService;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;
import software.amazon.awssdk.transfer.s3.model.Download;
import software.amazon.awssdk.transfer.s3.model.DownloadRequest;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static io.varada.cloudstorage.s3.S3Utils.handleAwsException;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;

public class S3CloudStorage
        extends CloudStorageService
{
    private final S3AsyncClient client;
    private final S3TransferManager transferManager;

    public S3CloudStorage(S3FileSystemFactory fileSystemFactory, S3AsyncClient client)
    {
        super(fileSystemFactory);
        this.client = requireNonNull(client, "client is null");
        this.transferManager = S3TransferManager.builder().s3Client(client).build();
    }

    @Override
    public void uploadFile(Location source, Location target)
            throws IOException
    {
        target.verifyValidFileLocation();

        S3Location s3Location = new S3Location(target);

        UploadRequest request = UploadRequest.builder()
                .putObjectRequest(req -> req.bucket(s3Location.bucket())
                        .key(s3Location.key())
                        .serverSideEncryption(AES256))
                .requestBody(AsyncRequestBody.fromFile(new File(source.toString())))
                .build();

        try {
            Upload upload = transferManager.upload(request);
            upload.completionFuture().join();
        }
        catch (RuntimeException e) {
            throw handleAwsException(e, "upload failed", s3Location);
        }
    }

    @Override
    public void downloadFile(Location source, Location target)
            throws IOException
    {
        source.verifyValidFileLocation();

        S3Location s3Location = new S3Location(source);

        DownloadRequest<GetObjectResponse> request =
                DownloadRequest.builder()
                        .getObjectRequest(req -> req.bucket(s3Location.bucket()).key(s3Location.key()))
                        .responseTransformer(AsyncResponseTransformer.toFile(new File(target.toString())))
                        .build();

        try {
            Download<GetObjectResponse> download = transferManager.download(request);
            download.completionFuture().join();
        }
        catch (RuntimeException e) {
            throw handleAwsException(e, "download failed", s3Location);
        }
    }

    @Override
    public void copyFile(Location source, Location destination)
            throws IOException
    {
        source.verifyValidFileLocation();
        destination.verifyValidFileLocation();

        S3Location sourceLocation = new S3Location(source);
        S3Location targetLocation = new S3Location(destination);

        CopyRequest request = CopyRequest.builder()
                .copyObjectRequest(req -> req.sourceBucket(sourceLocation.bucket())
                        .sourceKey(sourceLocation.key())
                        .destinationBucket(targetLocation.bucket())
                        .destinationKey(targetLocation.key())
                        .serverSideEncryption(AES256))
                .build();

        try {
            Copy copy = transferManager.copy(request);
            copy.completionFuture().join();
        }
        catch (RuntimeException e) {
            throw handleAwsException(e, "copy failed", sourceLocation);
        }
    }

    @Override
    public void copyFileReplaceTail(Location source, Location destination, long position, byte[] tailBuffer)
            throws IOException
    {
        source.verifyValidFileLocation();
        destination.verifyValidFileLocation();

        validateS3Location(source);
        validateS3Location(destination);

        try (S3AsyncOutput output = new S3AsyncOutput(client, source, destination)) {
            output.writeTail(position, tailBuffer);
        }
        catch (IOException e) {
            throw new IOException("copyFileReplaceTail failed", e);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        copyFile(source, target);
        deleteFile(source);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
    {
        try {
            listFiles(location);
            return Optional.of(true);
        }
        catch (IOException e) {
            return Optional.of(false);
        }
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateS3Location(Location location)
    {
        new S3Location(location);
    }
}
