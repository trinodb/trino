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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.units.Duration;
import io.trino.filesystem.EmulatedListFilesStartingFromIterator;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RecordSuccessRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.OptionalObjectAttributes;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Multimaps.toMultimap;
import static io.trino.filesystem.TrinoFileSystem.checkStartingFrom;
import static io.trino.filesystem.s3.S3Exceptions.handleS3Exception;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3ChecksumAlgorithm.toChecksumAlgorithm;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.NONE;
import static io.trino.filesystem.s3.S3SseCUtils.encoded;
import static io.trino.filesystem.s3.S3SseCUtils.md5Checksum;
import static io.trino.filesystem.s3.S3SseRequestConfigurator.setEncryptionSettings;
import static java.util.Objects.requireNonNull;

final class S3FileSystem
        implements TrinoFileSystem
{
    static final int DELETE_BATCH_SIZE = 1000;

    private final Executor uploadExecutor;
    private final S3Client client;
    private final S3Presigner preSigner;
    private final S3Context context;
    private final RequestPayer requestPayer;

    public S3FileSystem(Executor uploadExecutor, S3Client client, S3Presigner preSigner, S3Context context)
    {
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = requireNonNull(client, "client is null");
        this.preSigner = requireNonNull(preSigner, "preSigner is null");
        this.context = requireNonNull(context, "context is null");
        this.requestPayer = context.requestPayer();
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new S3InputFile(client, context, new S3Location(location), null, null, Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new S3InputFile(client, context, new S3Location(location), length, null, Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return new S3InputFile(client, context, new S3Location(location), length, lastModified, Optional.empty());
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return new S3InputFile(client, context, new S3Location(location), null, null, Optional.of(key));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return new S3InputFile(client, context, new S3Location(location), length, null, Optional.of(key));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return new S3InputFile(client, context, new S3Location(location), length, lastModified, Optional.of(key));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new S3OutputFile(uploadExecutor, client, context, new S3Location(location), Optional.empty());
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return new S3OutputFile(uploadExecutor, client, context, new S3Location(location), Optional.of(key));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        location.verifyValidFileLocation();
        S3Location s3Location = new S3Location(location);
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .requestPayer(requestPayer)
                .key(s3Location.key())
                .bucket(s3Location.bucket())
                .build();

        try {
            client.deleteObject(request);
        }
        catch (SdkException e) {
            throw handleS3Exception(e, "Failed to delete file: " + location);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        FileIterator iterator = listObjects(location, true, "");
        while (iterator.hasNext()) {
            List<Location> files = new ArrayList<>();
            while ((files.size() < 1000) && iterator.hasNext()) {
                files.add(iterator.next().location());
            }
            deleteObjects(files);
        }
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        locations.forEach(Location::verifyValidFileLocation);
        deleteObjects(locations);
    }

    private void deleteObjects(Collection<Location> locations)
            throws IOException
    {
        SetMultimap<String, String> bucketToKeys = locations.stream()
                .map(S3Location::new)
                .collect(toMultimap(S3Location::bucket, S3Location::key, HashMultimap::create));

        Map<String, String> failures = new HashMap<>();

        for (Entry<String, Collection<String>> entry : bucketToKeys.asMap().entrySet()) {
            String bucket = entry.getKey();
            Collection<String> allKeys = entry.getValue();

            for (List<String> keys : partition(allKeys, DELETE_BATCH_SIZE)) {
                List<ObjectIdentifier> objects = keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList();

                DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                        .overrideConfiguration(context::applyCredentialProviderOverride)
                        .requestPayer(requestPayer)
                        .bucket(bucket)
                        .checksumAlgorithm(toChecksumAlgorithm(context.checksumAlgorithm()))
                        .delete(builder -> builder.objects(objects).quiet(true))
                        .build();

                try {
                    for (S3Error error : deleteObjects(request)) {
                        String filePath = "s3://%s/%s".formatted(bucket, error.key());
                        if (error.message() == null) {
                            // If the error message is null, we just use the error code
                            failures.put(filePath, error.code());
                        }
                        else {
                            failures.put(filePath, "%s (%s)".formatted(error.message(), error.code()));
                        }
                    }
                }
                catch (SdkException e) {
                    throw handleS3Exception(e, "Error while batch deleting files");
                }
            }
        }

        if (!failures.isEmpty()) {
            throw new IOException("Failed to delete one or more files: " + failures);
        }
    }

    private List<S3Error> deleteObjects(DeleteObjectsRequest request)
            throws IOException
    {
        List<S3Error> failures = new ArrayList<>();
        // Keep per-key retries independent of the client's adaptive rate limiter.
        StandardRetryStrategy.Builder retryStrategyBuilder = AwsRetryStrategy.standardRetryStrategy().toBuilder();
        client.serviceClientConfiguration().overrideConfiguration().retryStrategy()
                .ifPresent(strategy -> retryStrategyBuilder.maxAttempts(strategy.maxAttempts()));
        try (RetryStrategy retryStrategy = retryStrategyBuilder.build()) {
            AcquireInitialTokenResponse initialToken = retryStrategy.acquireInitialToken(AcquireInitialTokenRequest.create(request.bucket()));
            RetryToken token = initialToken.token();
            Thread.sleep(initialToken.delay());

            while (true) {
                DeleteObjectsResponse response = client.deleteObjects(request);
                List<S3Error> retryableErrors = new ArrayList<>();
                for (S3Error error : response.errors()) {
                    if ("InternalError".equals(error.code())) {
                        retryableErrors.add(error);
                    }
                    else {
                        failures.add(error);
                    }
                }
                if (retryableErrors.isEmpty()) {
                    retryStrategy.recordSuccess(RecordSuccessRequest.create(token));
                    return failures;
                }

                // DeleteObjects reports per-key errors in an HTTP 200 response, outside the SDK's retry loop.
                SdkException failure = S3Exception.builder()
                        .statusCode(500)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalError").build())
                        .build();
                RefreshRetryTokenResponse retry;
                try {
                    retry = retryStrategy.refreshRetryToken(RefreshRetryTokenRequest.builder().token(token).failure(failure).build());
                }
                catch (TokenAcquisitionFailedException _) {
                    failures.addAll(retryableErrors);
                    return failures;
                }
                token = retry.token();
                Thread.sleep(retry.delay());

                Set<String> retryKeys = retryableErrors.stream().map(S3Error::key).collect(toImmutableSet());
                request = request.toBuilder()
                        .delete(request.delete().toBuilder().objects(request.delete().objects().stream()
                                .filter(object -> retryKeys.contains(object.key()))
                                .toList()).build())
                        .build();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException failure = new InterruptedIOException("Interrupted while batch deleting files");
            failure.initCause(e);
            throw failure;
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new IOException("S3 does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return listObjects(location, false, "");
    }

    @Override
    public FileIterator listFilesStartingFrom(Location location, String startingFrom)
            throws IOException
    {
        checkStartingFrom(startingFrom);
        return listObjects(location, false, startingFrom);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        validateS3Location(location);
        if (location.path().isEmpty() || listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
    {
        validateS3Location(location);
        // S3 does not have directories
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new IOException("S3 does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        S3Location s3Location = new S3Location(location);
        Location baseLocation = s3Location.baseLocation();

        ListObjectsV2Request request = listObjectsRequest(s3Location, directoryKey(s3Location.key()))
                .delimiter("/")
                .build();

        try {
            return client.listObjectsV2Paginator(request)
                    .commonPrefixes().stream()
                    .map(CommonPrefix::prefix)
                    .map(baseLocation::appendPath)
                    .collect(toImmutableSet());
        }
        catch (SdkException e) {
            throw handleS3Exception(e, "Failed to list location: " + location);
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
    {
        validateS3Location(targetPath);
        // S3 does not have directories
        return Optional.empty();
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return encryptedPreSignedUri(location, ttl, Optional.empty());
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return encryptedPreSignedUri(location, ttl, Optional.of(key));
    }

    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, Optional<EncryptionKey> key)
            throws IOException
    {
        location.verifyValidFileLocation();
        S3Location s3Location = new S3Location(location);

        verify(key.isEmpty() || context.s3SseContext().sseType() == NONE, "Encryption key cannot be used with SSE configuration");

        GetObjectRequest request = GetObjectRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .requestPayer(requestPayer)
                .key(s3Location.key())
                .bucket(s3Location.bucket())
                .applyMutation(builder ->
                        key.ifPresentOrElse(
                                encryption -> builder.sseCustomerKeyMD5(md5Checksum(encryption))
                                        .sseCustomerAlgorithm(encryption.algorithm())
                                        .sseCustomerKey(encoded(encryption)),
                                () -> setEncryptionSettings(builder, context.s3SseContext())))
                .build();

        GetObjectPresignRequest preSignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(ttl.toJavaTime())
                .getObjectRequest(request)
                .build();
        try {
            PresignedGetObjectRequest preSigned = preSigner.presignGetObject(preSignRequest);
            return Optional.of(new UriLocation(preSigned.url().toURI(), filterHeaders(preSigned.httpRequest().headers())));
        }
        catch (SdkException e) {
            throw handleS3Exception(e, "Failed to generate pre-signed URI");
        }
        catch (URISyntaxException e) {
            throw new TrinoFileSystemException("Failed to convert pre-signed URI to URI", e);
        }
    }

    private FileIterator listObjects(Location location, boolean includeDirectoryObjects, String startingFrom)
            throws IOException
    {
        S3Location s3Location = new S3Location(location);
        String keyPrefix = directoryKey(s3Location.key());
        ListObjectsV2Request.Builder request = listObjectsRequest(s3Location, keyPrefix);

        try {
            if (!startingFrom.isEmpty()) {
                // S3 startAfter is exclusive. Start slightly earlier and filter client-side to
                // preserve the inclusive listFilesStartingFrom semantics.
                String startingAfter = keyPrefix + truncateLastCodePoint(startingFrom);
                if (!startingAfter.isEmpty()) {
                    request.startAfter(startingAfter);
                }
            }

            Stream<S3Object> s3ObjectStream = client.listObjectsV2Paginator(request.build()).contents().stream();
            if (!includeDirectoryObjects) {
                s3ObjectStream = s3ObjectStream.filter(object -> !object.key().endsWith("/"));
            }

            S3FileIterator iterator = new S3FileIterator(s3Location, s3ObjectStream.iterator());
            if (!startingFrom.isEmpty()) {
                return new EmulatedListFilesStartingFromIterator(iterator, s3Location.location(), startingFrom);
            }
            return iterator;
        }
        catch (SdkException e) {
            throw handleS3Exception(e, "Failed to list location: " + location);
        }
    }

    private ListObjectsV2Request.Builder listObjectsRequest(S3Location location, String keyPrefix)
    {
        return ListObjectsV2Request.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                // Restore status will only be added to the response if requested
                .optionalObjectAttributes(OptionalObjectAttributes.RESTORE_STATUS)
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .prefix(keyPrefix);
    }

    private static String directoryKey(String key)
    {
        if (!key.isEmpty() && !key.endsWith("/")) {
            return key + "/";
        }
        return key;
    }

    private static String truncateLastCodePoint(String value)
    {
        verify(!value.isEmpty(), "value is empty");
        return value.substring(0, value.offsetByCodePoints(value.length(), -1));
    }

    private static Map<String, List<String>> filterHeaders(Map<String, List<String>> headers)
    {
        return headers.entrySet().stream()
                .filter(entry -> !entry.getKey().equalsIgnoreCase("host"))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateS3Location(Location location)
    {
        new S3Location(location);
    }
}
