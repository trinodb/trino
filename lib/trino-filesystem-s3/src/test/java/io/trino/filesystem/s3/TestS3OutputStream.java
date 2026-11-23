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
import io.trino.filesystem.s3.S3Context.S3SseContext;
import io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl;
import io.trino.filesystem.s3.S3FileSystemConfig.S3ChecksumAlgorithm;
import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestS3OutputStream
{
    private static final int PART_SIZE = 5 * 1024 * 1024;
    private static final S3Context CONTEXT = new S3Context(
            PART_SIZE,
            false,
            S3SseContext.of(S3SseType.NONE, null, null),
            Optional.empty(),
            StorageClassType.STANDARD,
            ObjectCannedAcl.NONE,
            S3ChecksumAlgorithm.DEFAULT);

    @Test
    void testCloseAbortsMultipartUploadWhenCompletionFails()
            throws IOException
    {
        RuntimeException completionFailure = new RuntimeException("completion failed");
        MultipartUploadS3Client client = new MultipartUploadS3Client(completionFailure);
        S3OutputFile outputFile = new S3OutputFile(directExecutor(), client, CONTEXT, new S3Location(Location.of("s3://bucket/key")), Optional.empty());

        OutputStream outputStream = outputFile.create();
        // writing more than one part starts a multipart upload
        outputStream.write(new byte[PART_SIZE + 1]);

        assertThatThrownBy(outputStream::close).isSameAs(completionFailure);
        assertThat(client.abortedUploadIds()).containsExactly("upload-id");
    }

    private static final class MultipartUploadS3Client
            implements S3Client
    {
        private final RuntimeException completionFailure;
        private final List<String> abortedUploadIds = new ArrayList<>();

        public MultipartUploadS3Client(RuntimeException completionFailure)
        {
            this.completionFailure = requireNonNull(completionFailure, "completionFailure is null");
        }

        public List<String> abortedUploadIds()
        {
            return abortedUploadIds;
        }

        @Override
        public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest request)
        {
            return CreateMultipartUploadResponse.builder().uploadId("upload-id").build();
        }

        @Override
        public UploadPartResponse uploadPart(UploadPartRequest request, RequestBody requestBody)
        {
            return UploadPartResponse.builder().eTag("etag-" + request.partNumber()).build();
        }

        @Override
        public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request)
        {
            throw completionFailure;
        }

        @Override
        public AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest request)
        {
            abortedUploadIds.add(request.uploadId());
            return AbortMultipartUploadResponse.builder().build();
        }

        @Override
        public String serviceName()
        {
            return SERVICE_NAME;
        }

        @Override
        public void close() {}
    }
}
