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
package io.trino.plugin.hive.s3;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.auth.SignerFactory;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3Encryption;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import com.google.common.net.MediaType;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.util.FSDataInputStreamTail;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.amazonaws.regions.Regions.US_EAST_1;
import static com.amazonaws.services.s3.Headers.CRYPTO_KEYWRAP_ALGORITHM;
import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION;
import static com.amazonaws.services.s3.Headers.UNENCRYPTED_CONTENT_LENGTH;
import static com.amazonaws.services.s3.model.StorageClass.DeepArchive;
import static com.amazonaws.services.s3.model.StorageClass.Glacier;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.hash.Hashing.md5;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.aws.AwsCurrentRegionHolder.getCurrentRegionFromEC2Metadata;
import static io.trino.plugin.hive.util.RetryDriver.retry;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.FSExceptionMessages.CANNOT_SEEK_PAST_EOF;
import static org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK;
import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;

public class TrinoS3FileSystem
        extends FileSystem
{
    public static final String S3_USER_AGENT_PREFIX = "trino.s3.user-agent-prefix";
    public static final String S3_CREDENTIALS_PROVIDER = "trino.s3.credentials-provider";
    public static final String S3_SSE_TYPE = "trino.s3.sse.type";
    public static final String S3_SSE_ENABLED = "trino.s3.sse.enabled";
    public static final String S3_SSE_KMS_KEY_ID = "trino.s3.sse.kms-key-id";
    public static final String S3_KMS_KEY_ID = "trino.s3.kms-key-id";
    public static final String S3_ENCRYPTION_MATERIALS_PROVIDER = "trino.s3.encryption-materials-provider";
    public static final String S3_PIN_CLIENT_TO_CURRENT_REGION = "trino.s3.pin-client-to-current-region";
    public static final String S3_MULTIPART_MIN_PART_SIZE = "trino.s3.multipart.min-part-size";
    public static final String S3_MULTIPART_MIN_FILE_SIZE = "trino.s3.multipart.min-file-size";
    public static final String S3_STAGING_DIRECTORY = "trino.s3.staging-directory";
    public static final String S3_MAX_CONNECTIONS = "trino.s3.max-connections";
    public static final String S3_SOCKET_TIMEOUT = "trino.s3.socket-timeout";
    public static final String S3_CONNECT_TIMEOUT = "trino.s3.connect-timeout";
    public static final String S3_MAX_RETRY_TIME = "trino.s3.max-retry-time";
    public static final String S3_MAX_BACKOFF_TIME = "trino.s3.max-backoff-time";
    public static final String S3_MAX_CLIENT_RETRIES = "trino.s3.max-client-retries";
    public static final String S3_MAX_ERROR_RETRIES = "trino.s3.max-error-retries";
    public static final String S3_SSL_ENABLED = "trino.s3.ssl.enabled";
    public static final String S3_PATH_STYLE_ACCESS = "trino.s3.path-style-access";
    public static final String S3_SIGNER_TYPE = "trino.s3.signer-type";
    public static final String S3_SIGNER_CLASS = "trino.s3.signer-class";
    public static final String S3_ENDPOINT = "trino.s3.endpoint";
    public static final String S3_SECRET_KEY = "trino.s3.secret-key";
    public static final String S3_ACCESS_KEY = "trino.s3.access-key";
    public static final String S3_SESSION_TOKEN = "trino.s3.session-token";
    public static final String S3_IAM_ROLE = "trino.s3.iam-role";
    public static final String S3_EXTERNAL_ID = "trino.s3.external-id";
    public static final String S3_ACL_TYPE = "trino.s3.upload-acl-type";
    public static final String S3_SKIP_GLACIER_OBJECTS = "trino.s3.skip-glacier-objects";
    public static final String S3_REQUESTER_PAYS_ENABLED = "trino.s3.requester-pays.enabled";
    public static final String S3_STREAMING_UPLOAD_ENABLED = "trino.s3.streaming.enabled";
    public static final String S3_STREAMING_UPLOAD_PART_SIZE = "trino.s3.streaming.part-size";
    public static final String S3_STORAGE_CLASS = "trino.s3.storage-class";

    private static final Logger log = Logger.get(TrinoS3FileSystem.class);
    private static final TrinoS3FileSystemStats STATS = new TrinoS3FileSystemStats();
    private static final RequestMetricCollector METRIC_COLLECTOR = new TrinoS3FileSystemMetricCollector(STATS);
    private static final String DIRECTORY_SUFFIX = "_$folder$";
    private static final DataSize BLOCK_SIZE = DataSize.of(32, MEGABYTE);
    private static final DataSize MAX_SKIP_SIZE = DataSize.of(1, MEGABYTE);
    private static final String PATH_SEPARATOR = "/";
    private static final Duration BACKOFF_MIN_SLEEP = new Duration(1, SECONDS);
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final String S3_CUSTOM_SIGNER = "TrinoS3CustomSigner";
    private static final Set<String> GLACIER_STORAGE_CLASSES = ImmutableSet.of(Glacier.toString(), DeepArchive.toString());
    private static final MediaType DIRECTORY_MEDIA_TYPE = MediaType.create("application", "x-directory");

    private URI uri;
    private Path workingDirectory;
    private AmazonS3 s3;
    private AWSCredentialsProvider credentialsProvider;
    private File stagingDirectory;
    private int maxAttempts;
    private Duration maxBackoffTime;
    private Duration maxRetryTime;
    private String iamRole;
    private String externalId;
    private boolean pinS3ClientToCurrentRegion;
    private boolean sseEnabled;
    private TrinoS3SseType sseType;
    private String sseKmsKeyId;
    private boolean isPathStyleAccess;
    private long multiPartUploadMinFileSize;
    private long multiPartUploadMinPartSize;
    private TrinoS3AclType s3AclType;
    private boolean skipGlacierObjects;
    private boolean requesterPaysEnabled;
    private boolean streamingUploadEnabled;
    private int streamingUploadPartSize;
    private TrinoS3StorageClass s3StorageClass;

    private final ExecutorService uploadExecutor = newCachedThreadPool(threadsNamed("s3-upload-%s"));

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(conf, "conf is null");
        super.initialize(uri, conf);
        setConf(conf);

        try {
            this.uri = new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid uri: " + uri, e);
        }
        this.workingDirectory = new Path(PATH_SEPARATOR).makeQualified(this.uri, new Path(PATH_SEPARATOR));

        HiveS3Config defaults = new HiveS3Config();
        this.stagingDirectory = new File(conf.get(S3_STAGING_DIRECTORY, defaults.getS3StagingDirectory().getPath()));
        this.maxAttempts = conf.getInt(S3_MAX_CLIENT_RETRIES, defaults.getS3MaxClientRetries()) + 1;
        this.maxBackoffTime = Duration.valueOf(conf.get(S3_MAX_BACKOFF_TIME, defaults.getS3MaxBackoffTime().toString()));
        this.maxRetryTime = Duration.valueOf(conf.get(S3_MAX_RETRY_TIME, defaults.getS3MaxRetryTime().toString()));
        int maxErrorRetries = conf.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = conf.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        Duration connectTimeout = Duration.valueOf(conf.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(conf.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = conf.getInt(S3_MAX_CONNECTIONS, defaults.getS3MaxConnections());
        this.multiPartUploadMinFileSize = conf.getLong(S3_MULTIPART_MIN_FILE_SIZE, defaults.getS3MultipartMinFileSize().toBytes());
        this.multiPartUploadMinPartSize = conf.getLong(S3_MULTIPART_MIN_PART_SIZE, defaults.getS3MultipartMinPartSize().toBytes());
        this.isPathStyleAccess = conf.getBoolean(S3_PATH_STYLE_ACCESS, defaults.isS3PathStyleAccess());
        this.iamRole = conf.get(S3_IAM_ROLE, defaults.getS3IamRole());
        this.externalId = conf.get(S3_EXTERNAL_ID, defaults.getS3ExternalId());
        this.pinS3ClientToCurrentRegion = conf.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        verify(!pinS3ClientToCurrentRegion || conf.get(S3_ENDPOINT) == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");
        this.sseEnabled = conf.getBoolean(S3_SSE_ENABLED, defaults.isS3SseEnabled());
        this.sseType = TrinoS3SseType.valueOf(conf.get(S3_SSE_TYPE, defaults.getS3SseType().name()));
        this.sseKmsKeyId = conf.get(S3_SSE_KMS_KEY_ID, defaults.getS3SseKmsKeyId());
        this.s3AclType = TrinoS3AclType.valueOf(conf.get(S3_ACL_TYPE, defaults.getS3AclType().name()));
        String userAgentPrefix = conf.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        this.skipGlacierObjects = conf.getBoolean(S3_SKIP_GLACIER_OBJECTS, defaults.isSkipGlacierObjects());
        this.requesterPaysEnabled = conf.getBoolean(S3_REQUESTER_PAYS_ENABLED, defaults.isRequesterPaysEnabled());
        this.streamingUploadEnabled = conf.getBoolean(S3_STREAMING_UPLOAD_ENABLED, defaults.isS3StreamingUploadEnabled());
        this.streamingUploadPartSize = toIntExact(conf.getLong(S3_STREAMING_UPLOAD_PART_SIZE, defaults.getS3StreamingPartSize().toBytes()));
        this.s3StorageClass = conf.getEnum(S3_STORAGE_CLASS, defaults.getS3StorageClass());

        ClientConfiguration configuration = new ClientConfiguration()
                .withMaxErrorRetry(maxErrorRetries)
                .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
                .withConnectionTimeout(toIntExact(connectTimeout.toMillis()))
                .withSocketTimeout(toIntExact(socketTimeout.toMillis()))
                .withMaxConnections(maxConnections)
                .withUserAgentPrefix(userAgentPrefix)
                .withUserAgentSuffix("Trino");

        this.credentialsProvider = createAwsCredentialsProvider(uri, conf);
        this.s3 = createAmazonS3Client(conf, configuration);
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(super::close);
            if (credentialsProvider instanceof Closeable) {
                closer.register((Closeable) credentialsProvider);
            }
            closer.register(uploadExecutor::shutdown);
            closer.register(s3::shutdown);
        }
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        workingDirectory = path;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        STATS.newListStatusCall();
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return toArray(list, LocatedFileStatus.class);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
    {
        // Either a single level or full listing, depending on the recursive flag, no "directories"
        // included in either path
        return new S3ObjectsV2RemoteIterator(listPath(path, OptionalInt.empty(), recursive ? ListingMode.RECURSIVE_FILES_ONLY : ListingMode.SHALLOW_FILES_ONLY));
    }

    public RemoteIterator<LocatedFileStatus> listFilesByPrefix(Path prefix, boolean recursive)
    {
        // Either a single level or full listing, depending on the recursive flag, no "directories"
        // included in either path
        return new S3ObjectsV2RemoteIterator(listPrefix(keyFromPath(prefix), OptionalInt.empty(), recursive ? ListingMode.RECURSIVE_FILES_ONLY : ListingMode.SHALLOW_FILES_ONLY));
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path)
    {
        STATS.newListLocatedStatusCall();
        return new S3ObjectsV2RemoteIterator(listPath(path, OptionalInt.empty(), ListingMode.SHALLOW_ALL));
    }

    private static final class S3ObjectsV2RemoteIterator
            implements RemoteIterator<LocatedFileStatus>
    {
        private final Iterator<LocatedFileStatus> iterator;

        public S3ObjectsV2RemoteIterator(Iterator<LocatedFileStatus> iterator)
        {
            this.iterator = requireNonNull(iterator, "iterator is null");
        }

        @Override
        public boolean hasNext()
                throws IOException
        {
            try {
                return iterator.hasNext();
            }
            catch (AmazonClientException e) {
                throw new IOException(e);
            }
        }

        @Override
        public LocatedFileStatus next()
                throws IOException
        {
            try {
                return iterator.next();
            }
            catch (AmazonClientException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        if (path.getName().isEmpty()) {
            // the bucket root requires special handling
            if (getS3ObjectMetadata(path) != null) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        ObjectMetadata metadata = getS3ObjectMetadata(path);

        if (metadata == null) {
            // check if this path is a directory
            Iterator<LocatedFileStatus> iterator = listPath(path, OptionalInt.of(1), ListingMode.SHALLOW_ALL);
            if (iterator.hasNext()) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        return new FileStatus(
                getObjectSize(path, metadata),
                // Some directories (e.g. uploaded through S3 GUI) return a charset in the Content-Type header
                MediaType.parse(metadata.getContentType()).is(DIRECTORY_MEDIA_TYPE),
                1,
                BLOCK_SIZE.toBytes(),
                lastModifiedTime(metadata),
                qualifiedPath(path));
    }

    private long getObjectSize(Path path, ObjectMetadata metadata)
            throws IOException
    {
        Map<String, String> userMetadata = metadata.getUserMetadata();
        String length = userMetadata.get(UNENCRYPTED_CONTENT_LENGTH);
        if (userMetadata.containsKey(SERVER_SIDE_ENCRYPTION) && length == null) {
            throw new IOException(format("%s header is not set on an encrypted object: %s", UNENCRYPTED_CONTENT_LENGTH, path));
        }

        if (length != null) {
            return Long.parseLong(length);
        }

        long reportedObjectSize = metadata.getContentLength();
        // x-amz-unencrypted-content-length was not set, infer length for cse-kms encrypted objects by reading the tail until EOF
        if (s3 instanceof AmazonS3Encryption && "kms".equalsIgnoreCase(userMetadata.get(CRYPTO_KEYWRAP_ALGORITHM))) {
            try (FSDataInputStream in = open(path, FSDataInputStreamTail.MAX_SUPPORTED_PADDING_BYTES + 1)) {
                return FSDataInputStreamTail.readTailForFileSize(path.toString(), reportedObjectSize, in);
            }
        }
        return reportedObjectSize;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
    {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new TrinoS3InputStream(s3, getBucketName(uri), path, requesterPaysEnabled, maxAttempts, maxBackoffTime, maxRetryTime),
                        bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        // Ignore the overwrite flag, since Trino Hive connector *usually* writes to unique file names.
        // Checking for file existence is thus an unnecessary, expensive operation.
        return new FSDataOutputStream(createOutputStream(path), statistics);
    }

    private OutputStream createOutputStream(Path path)
            throws IOException
    {
        String bucketName = getBucketName(uri);
        String key = keyFromPath(qualifiedPath(path));

        if (streamingUploadEnabled) {
            Supplier<String> uploadIdFactory = () -> initMultipartUpload(bucketName, key).getUploadId();
            return new TrinoS3StreamingOutputStream(s3, bucketName, key, this::customizePutObjectRequest, uploadIdFactory, uploadExecutor, streamingUploadPartSize);
        }

        if (!stagingDirectory.exists()) {
            createDirectories(stagingDirectory.toPath());
        }
        if (!stagingDirectory.isDirectory()) {
            throw new IOException("Configured staging path is not a directory: " + stagingDirectory);
        }
        File tempFile = createTempFile(stagingDirectory.toPath(), "trino-s3-", ".tmp").toFile();
        return new TrinoS3StagingOutputStream(s3, bucketName, key, tempFile, this::customizePutObjectRequest, multiPartUploadMinFileSize, multiPartUploadMinPartSize);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
    {
        throw new UnsupportedOperationException("append");
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        boolean srcDirectory;
        try {
            srcDirectory = directory(src);
        }
        catch (FileNotFoundException e) {
            return false;
        }

        try {
            if (!directory(dst)) {
                // cannot copy a file to an existing file
                return false;
            }
            // move source under destination directory
            dst = new Path(dst, src.getName());
        }
        catch (FileNotFoundException e) {
            // destination does not exist
        }

        if (keysEqual(src, dst)) {
            return false;
        }

        if (srcDirectory) {
            for (FileStatus file : listStatus(src)) {
                rename(file.getPath(), new Path(dst, file.getPath().getName()));
            }
            deleteObject(keyFromPath(src) + DIRECTORY_SUFFIX);
        }
        else {
            s3.copyObject(new CopyObjectRequest(getBucketName(uri), keyFromPath(src), getBucketName(uri), keyFromPath(dst))
                    .withRequesterPays(requesterPaysEnabled));
            delete(src, true);
        }

        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        try {
            if (!directory(path)) {
                return deleteObject(keyFromPath(path));
            }
        }
        catch (FileNotFoundException e) {
            return false;
        }

        if (!recursive) {
            throw new IOException("Directory " + path + " is not empty");
        }

        for (FileStatus file : listStatus(path)) {
            delete(file.getPath(), true);
        }
        deleteObject(keyFromPath(path) + DIRECTORY_SUFFIX);

        return true;
    }

    private boolean directory(Path path)
            throws IOException
    {
        return getFileStatus(path).isDirectory();
    }

    private boolean deleteObject(String key)
    {
        try {
            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(getBucketName(uri), key);
            if (requesterPaysEnabled) {
                // TODO use deleteObjectRequest.setRequesterPays() when https://github.com/aws/aws-sdk-java/issues/1219 is fixed
                // currently the method exists, but is ineffective (doesn't set the required HTTP header)
                deleteObjectRequest.putCustomRequestHeader(Headers.REQUESTER_PAYS_HEADER, Constants.REQUESTER_PAYS);
            }

            s3.deleteObject(deleteObjectRequest);
            return true;
        }
        catch (AmazonClientException e) {
            return false;
        }
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
    {
        // no need to do anything for S3
        return true;
    }

    /**
     * Enum representing the valid listing modes. This could be two booleans (recursive, filesOnly) except
     * that (recursive=true, filesOnly=false) can't be translated directly to a natively supported behavior
     */
    private enum ListingMode
    {
        SHALLOW_ALL,
        SHALLOW_FILES_ONLY,
        RECURSIVE_FILES_ONLY;

        public boolean isFilesOnly()
        {
            return (this == SHALLOW_FILES_ONLY || this == RECURSIVE_FILES_ONLY);
        }
    }

    /**
     * List all objects rooted at the provided path.
     */
    private Iterator<LocatedFileStatus> listPath(Path path, OptionalInt initialMaxKeys, ListingMode mode)
    {
        String key = keyFromPath(path);
        if (!key.isEmpty()) {
            key += PATH_SEPARATOR;
        }

        return listPrefix(key, initialMaxKeys, mode);
    }

    /**
     * List all objects whose absolute path matches the provided prefix.
     */
    private Iterator<LocatedFileStatus> listPrefix(String prefix, OptionalInt initialMaxKeys, ListingMode mode)
    {
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(getBucketName(uri))
                .withPrefix(prefix)
                .withDelimiter(mode == ListingMode.RECURSIVE_FILES_ONLY ? null : PATH_SEPARATOR)
                .withMaxKeys(initialMaxKeys.isPresent() ? initialMaxKeys.getAsInt() : null)
                .withRequesterPays(requesterPaysEnabled);

        STATS.newListObjectsCall();
        Iterator<ListObjectsV2Result> listings = new AbstractSequentialIterator<>(s3.listObjectsV2(request))
        {
            @Override
            protected ListObjectsV2Result computeNext(ListObjectsV2Result previous)
            {
                if (!previous.isTruncated()) {
                    return null;
                }
                // Clear any max keys after the first batch completes
                request.withMaxKeys(null).setContinuationToken(previous.getNextContinuationToken());
                return s3.listObjectsV2(request);
            }
        };

        Iterator<LocatedFileStatus> results = Iterators.concat(Iterators.transform(listings, this::statusFromListing));
        if (mode.isFilesOnly()) {
            //  Even recursive listing can still contain empty "directory" objects, must filter them out
            results = Iterators.filter(results, LocatedFileStatus::isFile);
        }
        return results;
    }

    private Iterator<LocatedFileStatus> statusFromListing(ListObjectsV2Result listing)
    {
        List<String> prefixes = listing.getCommonPrefixes();
        List<S3ObjectSummary> objects = listing.getObjectSummaries();
        if (prefixes.isEmpty()) {
            return statusFromObjects(objects);
        }
        if (objects.isEmpty()) {
            return statusFromPrefixes(prefixes);
        }
        return Iterators.concat(
                statusFromPrefixes(prefixes),
                statusFromObjects(objects));
    }

    private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes)
    {
        List<LocatedFileStatus> list = new ArrayList<>(prefixes.size());
        for (String prefix : prefixes) {
            Path path = qualifiedPath(new Path(PATH_SEPARATOR + prefix));
            FileStatus status = new FileStatus(0, true, 1, 0, 0, path);
            list.add(createLocatedFileStatus(status));
        }
        return list.iterator();
    }

    private Iterator<LocatedFileStatus> statusFromObjects(List<S3ObjectSummary> objects)
    {
        // NOTE: for encrypted objects, S3ObjectSummary.size() used below is NOT correct,
        // however, to get the correct size we'd need to make an additional request to get
        // user metadata, and in this case it doesn't matter.
        return objects.stream()
                .filter(object -> !object.getKey().endsWith(PATH_SEPARATOR))
                .filter(object -> !skipGlacierObjects || !isGlacierObject(object))
                .filter(object -> !isHadoopFolderMarker(object))
                .map(object -> new FileStatus(
                        object.getSize(),
                        false,
                        1,
                        BLOCK_SIZE.toBytes(),
                        object.getLastModified().getTime(),
                        qualifiedPath(new Path(PATH_SEPARATOR + object.getKey()))))
                .map(this::createLocatedFileStatus)
                .iterator();
    }

    private static boolean isGlacierObject(S3ObjectSummary object)
    {
        return GLACIER_STORAGE_CLASSES.contains(object.getStorageClass());
    }

    private static boolean isHadoopFolderMarker(S3ObjectSummary object)
    {
        return object.getKey().endsWith("_$folder$");
    }

    /**
     * This exception is for stopping retries for S3 calls that shouldn't be retried.
     * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
     */
    public static class UnrecoverableS3OperationException
            extends IOException
    {
        public UnrecoverableS3OperationException(Path path, Throwable cause)
        {
            // append the path info to the message
            super(format("%s (Path: %s)", cause, path), cause);
        }
    }

    @VisibleForTesting
    ObjectMetadata getS3ObjectMetadata(Path path)
            throws IOException
    {
        String bucketName = getBucketName(uri);
        String key = keyFromPath(path);
        ObjectMetadata s3ObjectMetadata = getS3ObjectMetadata(path, bucketName, key);
        if (s3ObjectMetadata == null && !key.isEmpty()) {
            return getS3ObjectMetadata(path, bucketName, key + PATH_SEPARATOR);
        }
        return s3ObjectMetadata;
    }

    private ObjectMetadata getS3ObjectMetadata(Path path, String bucketName, String key)
            throws IOException
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
                    .onRetry(STATS::newGetMetadataRetry)
                    .run("getS3ObjectMetadata", () -> {
                        try {
                            STATS.newMetadataCall();
                            return s3.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key)
                                    .withRequesterPays(requesterPaysEnabled));
                        }
                        catch (RuntimeException e) {
                            STATS.newGetMetadataError();
                            if (e instanceof AmazonServiceException) {
                                switch (((AmazonServiceException) e).getStatusCode()) {
                                    case HTTP_FORBIDDEN:
                                    case HTTP_BAD_REQUEST:
                                        throw new UnrecoverableS3OperationException(path, e);
                                }
                            }
                            if (e instanceof AmazonS3Exception &&
                                    ((AmazonS3Exception) e).getStatusCode() == HTTP_NOT_FOUND) {
                                return null;
                            }
                            throw e;
                        }
                    });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private Path qualifiedPath(Path path)
    {
        return path.makeQualified(this.uri, getWorkingDirectory());
    }

    private LocatedFileStatus createLocatedFileStatus(FileStatus status)
    {
        try {
            BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
            return new LocatedFileStatus(status, fakeLocation);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long lastModifiedTime(ObjectMetadata metadata)
    {
        Date date = metadata.getLastModified();
        return (date != null) ? date.getTime() : 0;
    }

    private static boolean keysEqual(Path p1, Path p2)
    {
        return keyFromPath(p1).equals(keyFromPath(p2));
    }

    public static String keyFromPath(Path path)
    {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        String key = nullToEmpty(path.toUri().getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    private AmazonS3 createAmazonS3Client(Configuration hadoopConfig, ClientConfiguration clientConfig)
    {
        Optional<EncryptionMaterialsProvider> encryptionMaterialsProvider = createEncryptionMaterialsProvider(hadoopConfig);
        AmazonS3Builder<? extends AmazonS3Builder<?, ?>, ? extends AmazonS3> clientBuilder;

        String signerType = hadoopConfig.get(S3_SIGNER_TYPE);
        if (signerType != null) {
            clientConfig.withSignerOverride(signerType);
        }

        String signerClass = hadoopConfig.get(S3_SIGNER_CLASS);
        if (signerClass != null) {
            Class<? extends Signer> klass;
            try {
                klass = Class.forName(signerClass).asSubclass(Signer.class);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("Signer class not found: " + signerClass, e);
            }
            SignerFactory.registerSigner(S3_CUSTOM_SIGNER, klass);
            clientConfig.setSignerOverride(S3_CUSTOM_SIGNER);
        }

        if (encryptionMaterialsProvider.isPresent()) {
            clientBuilder = AmazonS3EncryptionClient.encryptionBuilder()
                    .withCredentials(credentialsProvider)
                    .withEncryptionMaterials(encryptionMaterialsProvider.get())
                    .withClientConfiguration(clientConfig)
                    .withMetricsCollector(METRIC_COLLECTOR);
        }
        else {
            clientBuilder = AmazonS3Client.builder()
                    .withCredentials(credentialsProvider)
                    .withClientConfiguration(clientConfig)
                    .withMetricsCollector(METRIC_COLLECTOR);
        }

        boolean regionOrEndpointSet = false;

        // use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            clientBuilder.setRegion(getCurrentRegionFromEC2Metadata().getName());
            regionOrEndpointSet = true;
        }

        String endpoint = hadoopConfig.get(S3_ENDPOINT);
        if (endpoint != null) {
            clientBuilder.setEndpointConfiguration(new EndpointConfiguration(endpoint, null));
            regionOrEndpointSet = true;
        }

        if (isPathStyleAccess) {
            clientBuilder.enablePathStyleAccess();
        }

        if (!regionOrEndpointSet) {
            clientBuilder.withRegion(US_EAST_1);
            clientBuilder.setForceGlobalBucketAccessEnabled(true);
        }

        return clientBuilder.build();
    }

    private static Optional<EncryptionMaterialsProvider> createEncryptionMaterialsProvider(Configuration hadoopConfig)
    {
        String kmsKeyId = hadoopConfig.get(S3_KMS_KEY_ID);
        if (kmsKeyId != null) {
            return Optional.of(new KMSEncryptionMaterialsProvider(kmsKeyId));
        }

        String empClassName = hadoopConfig.get(S3_ENCRYPTION_MATERIALS_PROVIDER);
        if (empClassName == null) {
            return Optional.empty();
        }

        try {
            Object instance = Class.forName(empClassName).getConstructor().newInstance();
            if (!(instance instanceof EncryptionMaterialsProvider)) {
                throw new RuntimeException("Invalid encryption materials provider class: " + instance.getClass().getName());
            }
            EncryptionMaterialsProvider emp = (EncryptionMaterialsProvider) instance;
            if (emp instanceof Configurable) {
                ((Configurable) emp).setConf(hadoopConfig);
            }
            return Optional.of(emp);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to load or create S3 encryption materials provider: " + empClassName, e);
        }
    }

    private AWSCredentialsProvider createAwsCredentialsProvider(URI uri, Configuration conf)
    {
        // credentials embedded in the URI take precedence and are used alone
        Optional<AWSCredentials> credentials = getEmbeddedAwsCredentials(uri);
        if (credentials.isPresent()) {
            return new AWSStaticCredentialsProvider(credentials.get());
        }

        // a custom credential provider is also used alone
        String providerClass = conf.get(S3_CREDENTIALS_PROVIDER);
        if (!isNullOrEmpty(providerClass)) {
            return getCustomAWSCredentialsProvider(uri, conf, providerClass);
        }

        // use configured credentials or default chain with optional role
        AWSCredentialsProvider provider = getAwsCredentials(conf)
                .map(value -> (AWSCredentialsProvider) new AWSStaticCredentialsProvider(value))
                .orElseGet(DefaultAWSCredentialsProviderChain::getInstance);

        if (iamRole != null) {
            provider = new STSAssumeRoleSessionCredentialsProvider.Builder(iamRole, "trino-session")
                    .withExternalId(externalId)
                    .withLongLivedCredentialsProvider(provider)
                    .build();
        }

        return provider;
    }

    private static AWSCredentialsProvider getCustomAWSCredentialsProvider(URI uri, Configuration conf, String providerClass)
    {
        try {
            log.debug("Using AWS credential provider %s for URI %s", providerClass, uri);
            return conf.getClassByName(providerClass)
                    .asSubclass(AWSCredentialsProvider.class)
                    .getConstructor(URI.class, Configuration.class)
                    .newInstance(uri, conf);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s for URI %s", providerClass, uri), e);
        }
    }

    private static Optional<AWSCredentials> getEmbeddedAwsCredentials(URI uri)
    {
        String userInfo = nullToEmpty(uri.getUserInfo());
        List<String> parts = Splitter.on(':').limit(2).splitToList(userInfo);
        if (parts.size() == 2) {
            String accessKey = parts.get(0);
            String secretKey = parts.get(1);
            if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
                return Optional.of(new BasicAWSCredentials(accessKey, secretKey));
            }
        }
        return Optional.empty();
    }

    private static Optional<AWSCredentials> getAwsCredentials(Configuration conf)
    {
        String accessKey = conf.get(S3_ACCESS_KEY);
        String secretKey = conf.get(S3_SECRET_KEY);

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {
            return Optional.empty();
        }

        String sessionToken = conf.get(S3_SESSION_TOKEN);
        if (!isNullOrEmpty(sessionToken)) {
            return Optional.of(new BasicSessionCredentials(accessKey, secretKey, sessionToken));
        }

        return Optional.of(new BasicAWSCredentials(accessKey, secretKey));
    }

    private void customizePutObjectRequest(PutObjectRequest request)
    {
        if (request.getMetadata() == null) {
            request.setMetadata(new ObjectMetadata());
        }
        if (sseEnabled) {
            switch (sseType) {
                case KMS:
                    request.setSSEAwsKeyManagementParams(getSseKeyManagementParams());
                    break;
                case S3:
                    request.getMetadata().setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                    break;
            }
        }
        request.setCannedAcl(s3AclType.getCannedACL());
        request.setRequesterPays(requesterPaysEnabled);
        request.setStorageClass(s3StorageClass.getS3StorageClass());
    }

    private InitiateMultipartUploadResult initMultipartUpload(String bucket, String key)
    {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key)
                .withObjectMetadata(new ObjectMetadata())
                .withCannedACL(s3AclType.getCannedACL())
                .withRequesterPays(requesterPaysEnabled)
                .withStorageClass(s3StorageClass.getS3StorageClass());

        if (sseEnabled) {
            switch (sseType) {
                case KMS:
                    request.setSSEAwsKeyManagementParams(getSseKeyManagementParams());
                    break;
                case S3:
                    request.getObjectMetadata().setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                    break;
            }
        }

        return s3.initiateMultipartUpload(request);
    }

    private SSEAwsKeyManagementParams getSseKeyManagementParams()
    {
        return (sseKmsKeyId != null) ? new SSEAwsKeyManagementParams(sseKmsKeyId) : new SSEAwsKeyManagementParams();
    }

    private static class TrinoS3InputStream
            extends FSInputStream
    {
        private final AmazonS3 s3;
        private final String bucket;
        private final Path path;
        private final boolean requesterPaysEnabled;
        private final int maxAttempts;
        private final Duration maxBackoffTime;
        private final Duration maxRetryTime;

        private final AtomicBoolean closed = new AtomicBoolean();

        private InputStream in;
        private long streamPosition;
        private long nextReadPosition;

        public TrinoS3InputStream(AmazonS3 s3, String bucket, Path path, boolean requesterPaysEnabled, int maxAttempts, Duration maxBackoffTime, Duration maxRetryTime)
        {
            this.s3 = requireNonNull(s3, "s3 is null");
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.path = requireNonNull(path, "path is null");
            this.requesterPaysEnabled = requesterPaysEnabled;

            checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
            this.maxAttempts = maxAttempts;
            this.maxBackoffTime = requireNonNull(maxBackoffTime, "maxBackoffTime is null");
            this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
        }

        @Override
        public void close()
        {
            closed.set(true);
            closeStream();
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            checkClosed();
            if (position < 0) {
                throw new EOFException(NEGATIVE_SEEK);
            }
            checkPositionIndexes(offset, offset + length, buffer.length);
            if (length == 0) {
                return 0;
            }

            try {
                return retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, EOFException.class)
                        .onRetry(STATS::newGetObjectRetry)
                        .run("getS3Object", () -> {
                            InputStream stream;
                            try {
                                GetObjectRequest request = new GetObjectRequest(bucket, keyFromPath(path))
                                        .withRange(position, (position + length) - 1)
                                        .withRequesterPays(requesterPaysEnabled);
                                stream = s3.getObject(request).getObjectContent();
                            }
                            catch (RuntimeException e) {
                                STATS.newGetObjectError();
                                if (e instanceof AmazonServiceException) {
                                    switch (((AmazonServiceException) e).getStatusCode()) {
                                        case HTTP_FORBIDDEN:
                                        case HTTP_BAD_REQUEST:
                                            throw new UnrecoverableS3OperationException(path, e);
                                    }
                                }
                                if (e instanceof AmazonS3Exception) {
                                    switch (((AmazonS3Exception) e).getStatusCode()) {
                                        case HTTP_RANGE_NOT_SATISFIABLE:
                                            throw new EOFException(CANNOT_SEEK_PAST_EOF);
                                        case HTTP_NOT_FOUND:
                                            throw new UnrecoverableS3OperationException(path, e);
                                    }
                                }
                                throw e;
                            }

                            STATS.connectionOpened();
                            try {
                                int read = 0;
                                while (read < length) {
                                    int n = stream.read(buffer, offset + read, length - read);
                                    if (n <= 0) {
                                        if (read > 0) {
                                            return read;
                                        }
                                        return -1;
                                    }
                                    read += n;
                                }
                                return read;
                            }
                            catch (Throwable t) {
                                STATS.newReadError(t);
                                abortStream(stream);
                                throw t;
                            }
                            finally {
                                STATS.connectionReleased();
                                stream.close();
                            }
                        });
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        @Override
        public void seek(long pos)
                throws IOException
        {
            checkClosed();
            if (pos < 0) {
                throw new EOFException(NEGATIVE_SEEK);
            }

            // this allows a seek beyond the end of the stream but the next read will fail
            nextReadPosition = pos;
        }

        @Override
        public long getPos()
        {
            return nextReadPosition;
        }

        @Override
        public int read()
        {
            // This stream is wrapped with BufferedInputStream, so this method should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            checkClosed();
            try {
                int bytesRead = retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, AbortedException.class)
                        .onRetry(STATS::newReadRetry)
                        .run("readStream", () -> {
                            seekStream();
                            try {
                                return in.read(buffer, offset, length);
                            }
                            catch (Exception e) {
                                STATS.newReadError(e);
                                closeStream();
                                throw e;
                            }
                        });

                if (bytesRead != -1) {
                    streamPosition += bytesRead;
                    nextReadPosition += bytesRead;
                }
                return bytesRead;
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos)
        {
            return false;
        }

        private void seekStream()
                throws IOException
        {
            if ((in != null) && (nextReadPosition == streamPosition)) {
                // already at specified position
                return;
            }

            if ((in != null) && (nextReadPosition > streamPosition)) {
                // seeking forwards
                long skip = nextReadPosition - streamPosition;
                if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
                    // already buffered or seek is small enough
                    try {
                        if (in.skip(skip) == skip) {
                            streamPosition = nextReadPosition;
                            return;
                        }
                    }
                    catch (IOException ignored) {
                        // will retry by re-opening the stream
                    }
                }
            }

            // close the stream and open at desired position
            streamPosition = nextReadPosition;
            closeStream();
            openStream();
        }

        private void openStream()
                throws IOException
        {
            if (in == null) {
                in = openStream(path, nextReadPosition);
                streamPosition = nextReadPosition;
                STATS.connectionOpened();
            }
        }

        private InputStream openStream(Path path, long start)
                throws IOException
        {
            try {
                return retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
                        .onRetry(STATS::newGetObjectRetry)
                        .run("getS3Object", () -> {
                            try {
                                GetObjectRequest request = new GetObjectRequest(bucket, keyFromPath(path))
                                        .withRange(start)
                                        .withRequesterPays(requesterPaysEnabled);
                                return s3.getObject(request).getObjectContent();
                            }
                            catch (RuntimeException e) {
                                STATS.newGetObjectError();
                                if (e instanceof AmazonServiceException) {
                                    switch (((AmazonServiceException) e).getStatusCode()) {
                                        case HTTP_FORBIDDEN:
                                        case HTTP_BAD_REQUEST:
                                            throw new UnrecoverableS3OperationException(path, e);
                                    }
                                }
                                if (e instanceof AmazonS3Exception) {
                                    switch (((AmazonS3Exception) e).getStatusCode()) {
                                        case HTTP_RANGE_NOT_SATISFIABLE:
                                            // ignore request for start past end of object
                                            return new ByteArrayInputStream(new byte[0]);
                                        case HTTP_NOT_FOUND:
                                            throw new UnrecoverableS3OperationException(path, e);
                                    }
                                }
                                throw e;
                            }
                        });
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        private void closeStream()
        {
            if (in != null) {
                abortStream(in);
                in = null;
                STATS.connectionReleased();
            }
        }

        private void checkClosed()
                throws IOException
        {
            if (closed.get()) {
                throw new IOException(STREAM_IS_CLOSED);
            }
        }

        private static void abortStream(InputStream in)
        {
            try {
                if (in instanceof S3ObjectInputStream) {
                    ((S3ObjectInputStream) in).abort();
                }
                else {
                    in.close();
                }
            }
            catch (IOException | AbortedException ignored) {
                // thrown if the current thread is in the interrupted state
            }
        }

        private static RuntimeException propagate(Exception e)
                throws IOException
        {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new IOException(e);
        }
    }

    private static class TrinoS3StagingOutputStream
            extends FilterOutputStream
    {
        private final TransferManager transferManager;
        private final String bucket;
        private final String key;
        private final File tempFile;
        private final Consumer<PutObjectRequest> requestCustomizer;

        private boolean closed;

        public TrinoS3StagingOutputStream(
                AmazonS3 s3,
                String bucket,
                String key,
                File tempFile,
                Consumer<PutObjectRequest> requestCustomizer,
                long multiPartUploadMinFileSize,
                long multiPartUploadMinPartSize)
                throws IOException
        {
            super(new BufferedOutputStream(new FileOutputStream(requireNonNull(tempFile, "tempFile is null"))));

            transferManager = TransferManagerBuilder.standard()
                    .withS3Client(requireNonNull(s3, "s3 is null"))
                    .withMinimumUploadPartSize(multiPartUploadMinPartSize)
                    .withMultipartUploadThreshold(multiPartUploadMinFileSize).build();

            this.bucket = requireNonNull(bucket, "bucket is null");
            this.key = requireNonNull(key, "key is null");
            this.tempFile = tempFile;
            this.requestCustomizer = requireNonNull(requestCustomizer, "requestCustomizer is null");

            log.debug("OutputStream for key '%s' using file: %s", key, tempFile);
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;

            try {
                super.close();
                uploadObject();
            }
            finally {
                if (!tempFile.delete()) {
                    log.warn("Could not delete temporary file: %s", tempFile);
                }
                // close transfer manager but keep underlying S3 client open
                transferManager.shutdownNow(false);
            }
        }

        private void uploadObject()
                throws IOException
        {
            try {
                log.debug("Starting upload for bucket: %s, key: %s, file: %s, size: %s", bucket, key, tempFile, tempFile.length());
                STATS.uploadStarted();

                PutObjectRequest request = new PutObjectRequest(bucket, key, tempFile);
                requestCustomizer.accept(request);

                Upload upload = transferManager.upload(request);

                if (log.isDebugEnabled()) {
                    upload.addProgressListener(createProgressListener(upload));
                }

                upload.waitForCompletion();
                STATS.uploadSuccessful();
                log.debug("Completed upload for bucket: %s, key: %s", bucket, key);
            }
            catch (AmazonClientException e) {
                STATS.uploadFailed();
                throw new IOException(e);
            }
            catch (InterruptedException e) {
                STATS.uploadFailed();
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }

        private ProgressListener createProgressListener(Transfer transfer)
        {
            return new ProgressListener()
            {
                private ProgressEventType previousType;
                private double previousTransferred;

                @Override
                public synchronized void progressChanged(ProgressEvent progressEvent)
                {
                    ProgressEventType eventType = progressEvent.getEventType();
                    if (previousType != eventType) {
                        log.debug("Upload progress event (%s/%s): %s", bucket, key, eventType);
                        previousType = eventType;
                    }

                    double transferred = transfer.getProgress().getPercentTransferred();
                    if (transferred >= (previousTransferred + 10.0)) {
                        log.debug("Upload percentage (%s/%s): %.0f%%", bucket, key, transferred);
                        previousTransferred = transferred;
                    }
                }
            };
        }
    }

    private static class TrinoS3StreamingOutputStream
            extends OutputStream
    {
        private final AmazonS3 s3;
        private final String bucketName;
        private final String key;
        private final Consumer<PutObjectRequest> requestCustomizer;
        private final Supplier<String> uploadIdFactory;
        private final ExecutorService uploadExecutor;

        private int currentPartNumber;
        private byte[] buffer;
        private int bufferSize;

        private boolean failed;
        private Optional<String> uploadId = Optional.empty();
        private Future<UploadPartResult> inProgressUploadFuture;
        private final List<UploadPartResult> parts = new ArrayList<>();

        public TrinoS3StreamingOutputStream(
                AmazonS3 s3,
                String bucketName,
                String key,
                Consumer<PutObjectRequest> requestCustomizer,
                Supplier<String> uploadIdFactory,
                ExecutorService uploadExecutor,
                int partSize)
        {
            STATS.uploadStarted();

            this.s3 = requireNonNull(s3, "s3 is null");

            this.buffer = new byte[partSize];

            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.requestCustomizer = requireNonNull(requestCustomizer, "requestCustomizer is null");
            this.uploadIdFactory = requireNonNull(uploadIdFactory, "uploadIdFactory is null");
            this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        }

        @Override
        public void write(int b)
                throws IOException
        {
            flushBuffer(false);
            buffer[bufferSize] = (byte) b;
            bufferSize++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            while (length > 0) {
                int copied = min(buffer.length - bufferSize, length);
                arraycopy(bytes, offset, buffer, bufferSize, copied);
                bufferSize += copied;

                flushBuffer(false);

                offset += copied;
                length -= copied;
            }
        }

        @Override
        public void flush()
                throws IOException
        {
            flushBuffer(false);
        }

        @Override
        public void close()
                throws IOException
        {
            if (failed) {
                try {
                    abortUpload();
                    return;
                }
                catch (RuntimeException e) {
                    throw new IOException(e);
                }
            }

            try {
                flushBuffer(true);
                waitForPreviousUploadFinish();
            }
            catch (IOException | RuntimeException e) {
                abortUploadSuppressed(e);
                throw e;
            }

            try {
                uploadId.ifPresent(this::finishUpload);
            }
            catch (RuntimeException e) {
                abortUploadSuppressed(e);
                throw new IOException(e);
            }
        }

        private void flushBuffer(boolean finished)
                throws IOException
        {
            try {
                waitForPreviousUploadFinish();
            }
            catch (IOException e) {
                failed = true;
                abortUploadSuppressed(e);
                throw e;
            }

            // skip multipart upload if there would only be one part
            if (finished && uploadId.isEmpty()) {
                InputStream in = new ByteArrayInputStream(buffer, 0, bufferSize);

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(bufferSize);
                metadata.setContentMD5(getMd5AsBase64(buffer, 0, bufferSize));

                PutObjectRequest request = new PutObjectRequest(bucketName, key, in, metadata);
                requestCustomizer.accept(request);

                try {
                    s3.putObject(request);
                    return;
                }
                catch (AmazonServiceException e) {
                    failed = true;
                    throw new IOException(e);
                }
            }

            // The multipart upload API only accept the last part to be less than 5MB
            if (bufferSize == buffer.length || (finished && bufferSize > 0)) {
                byte[] data = buffer;
                int length = bufferSize;

                if (finished) {
                    this.buffer = null;
                }
                else {
                    this.buffer = new byte[buffer.length];
                    bufferSize = 0;
                }

                inProgressUploadFuture = uploadExecutor.submit(() -> uploadPage(data, length));
            }
        }

        private void waitForPreviousUploadFinish()
                throws IOException
        {
            if (inProgressUploadFuture == null) {
                return;
            }

            try {
                inProgressUploadFuture.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
            catch (ExecutionException e) {
                throw new IOException("Streaming upload failed", e);
            }
        }

        private UploadPartResult uploadPage(byte[] data, int length)
        {
            if (uploadId.isEmpty()) {
                uploadId = Optional.of(uploadIdFactory.get());
            }

            currentPartNumber++;
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withUploadId(uploadId.get())
                    .withPartNumber(currentPartNumber)
                    .withInputStream(new ByteArrayInputStream(data, 0, length))
                    .withPartSize(length)
                    .withMD5Digest(getMd5AsBase64(data, 0, length));

            UploadPartResult partResult = s3.uploadPart(uploadRequest);
            parts.add(partResult);
            return partResult;
        }

        private void finishUpload(String uploadId)
        {
            List<PartETag> etags = parts.stream()
                    .map(UploadPartResult::getPartETag)
                    .collect(toList());
            s3.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, etags));

            STATS.uploadSuccessful();
        }

        private void abortUpload()
        {
            STATS.uploadFailed();

            uploadId.ifPresent(id -> s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, id)));
        }

        @SuppressWarnings("ObjectEquality")
        private void abortUploadSuppressed(Throwable throwable)
        {
            try {
                abortUpload();
            }
            catch (Throwable t) {
                if (throwable != t) {
                    throwable.addSuppressed(t);
                }
            }
        }
    }

    @VisibleForTesting
    AmazonS3 getS3Client()
    {
        return s3;
    }

    @VisibleForTesting
    void setS3Client(AmazonS3 client)
    {
        s3 = client;
    }

    @VisibleForTesting
    protected String getBucketName(URI uri)
    {
        return extractBucketName(uri);
    }

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    public static String extractBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    public static TrinoS3FileSystemStats getFileSystemStats()
    {
        return STATS;
    }

    private static String getMd5AsBase64(byte[] data, int offset, int length)
    {
        @SuppressWarnings("deprecation")
        byte[] md5 = md5().hashBytes(data, offset, length).asBytes();
        return Base64.getEncoder().encodeToString(md5);
    }
}
