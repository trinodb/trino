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
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.RestoreStatus;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.util.Objects.requireNonNull;

final class S3FileSystem
        implements TrinoFileSystem
{
    private static final Set<ObjectStorageClass> GLACIER_STORAGE_CLASSES = Set.of(ObjectStorageClass.GLACIER, ObjectStorageClass.DEEP_ARCHIVE);

    private final S3Client client;
    private final S3Context context;
    private final RequestPayer requestPayer;

    public S3FileSystem(S3Client client, S3Context context)
    {
        this.client = requireNonNull(client, "client is null");
        this.context = requireNonNull(context, "context is null");
        this.requestPayer = context.requestPayer();
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new S3InputFile(client, context, new S3Location(location), null);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new S3InputFile(client, context, new S3Location(location), length);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new S3OutputFile(client, context, new S3Location(location));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        location.verifyValidFileLocation();
        S3Location s3Location = new S3Location(location);
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .requestPayer(requestPayer)
                .key(s3Location.key())
                .bucket(s3Location.bucket())
                .build();

        try {
            client.deleteObject(request);
        }
        catch (SdkException e) {
            throw new IOException("Failed to delete file: " + location, e);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        FileIterator iterator = listFiles(location);
        while (iterator.hasNext()) {
            List<Location> files = new ArrayList<>();
            while ((files.size() < 1000) && iterator.hasNext()) {
                files.add(iterator.next().location());
            }
            deleteFiles(files);
        }
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        locations.forEach(Location::verifyValidFileLocation);

        SetMultimap<String, String> bucketToKeys = locations.stream()
                .map(S3Location::new)
                .collect(toMultimap(S3Location::bucket, S3Location::key, HashMultimap::create));

        Map<String, String> failures = new HashMap<>();

        for (Entry<String, Collection<String>> entry : bucketToKeys.asMap().entrySet()) {
            String bucket = entry.getKey();
            Collection<String> allKeys = entry.getValue();

            for (List<String> keys : partition(allKeys, 250)) {
                List<ObjectIdentifier> objects = keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList();

                DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                        .requestPayer(requestPayer)
                        .bucket(bucket)
                        .delete(builder -> builder.objects(objects).quiet(true))
                        .build();

                try {
                    DeleteObjectsResponse response = client.deleteObjects(request);
                    for (S3Error error : response.errors()) {
                        failures.put("s3://%s/%s".formatted(bucket, error.key()), error.code());
                    }
                }
                catch (SdkException e) {
                    throw new IOException("Error while batch deleting files", e);
                }
            }
        }

        if (!failures.isEmpty()) {
            throw new IOException("Failed to delete one or more files: " + failures);
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
        S3Location s3Location = new S3Location(location);

        String key = s3Location.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(s3Location.bucket())
                .prefix(key)
                .build();

        try {
            Iterator<S3Object> iterator = client.listObjectsV2Paginator(request).contents()
                    .stream()
                    .filter(this::shouldReadObject)
                    .iterator();
            return new S3FileIterator(s3Location, iterator);
        }
        catch (SdkException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
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

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateS3Location(Location location)
    {
        new S3Location(location);
    }

    private boolean shouldReadObject(S3Object object)
    {
        return switch (context.s3ObjectStorageClassFilter()) {
            case READ_ALL -> true;
            case SKIP_ALL_GLACIER -> !isGlacierObject(object);
            case READ_RESTORED_GLACIER_OBJECTS -> !isGlacierObject(object) || isCompletedRestoredObject(object);
        };
    }

    private static boolean isCompletedRestoredObject(S3Object object)
    {
        /* There are 3 cases for the restore status:
         *
         * 1. The object is not restored, and has not been requested to be restored. We should have a null
         * value for restoreStatus
         * 2. The object is in the process of being restored. isRestoreInProgress will be true, but restoreExpiryDate
         * will be null.
         * 3. The object has completed restore. isRestoreInProgress will be false, restoreExpiryDate will be some date.
         *
         * Since we only care about distinguishing when it is case 3, we can just check restoreExpiryDate for a non-null value. */
        return Optional.ofNullable(object.restoreStatus())
                .map(RestoreStatus::restoreExpiryDate)
                .isPresent();
    }

    private static boolean isGlacierObject(S3Object object)
    {
        return GLACIER_STORAGE_CLASSES.contains(object.storageClass());
    }
}
