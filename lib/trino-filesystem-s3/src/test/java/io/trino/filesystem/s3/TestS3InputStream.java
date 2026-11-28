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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

final class TestS3InputStream
{
    private static final Location LOCATION = Location.of("s3://bucket/key");
    private static final int FILE_SIZE = 4 * 1024 * 1024;

    @Test
    void testForwardSeekNearEndAbortsLargeRemainder()
            throws IOException
    {
        FakeS3Client client = new FakeS3Client(new byte[FILE_SIZE]);
        try (S3InputStream input = newInputStream(client)) {
            assertThat(input.read()).isEqualTo(0);

            // seek forward beyond the skip limit to a position close to the end of the file
            input.seek(FILE_SIZE - 100);
            assertThat(input.read()).isEqualTo(0);
        }

        assertThat(client.responses()).hasSize(2);
        TrackingStream first = client.responses().getFirst();
        // the first stream has almost the whole file remaining, so it must be aborted rather than drained
        assertThat(first.aborted()).isTrue();
        assertThat(first.bytesRead()).isEqualTo(1);
    }

    @Test
    void testBackwardSeekDrainsSmallRemainder()
            throws IOException
    {
        FakeS3Client client = new FakeS3Client(new byte[FILE_SIZE]);
        try (S3InputStream input = newInputStream(client)) {
            input.seek(FILE_SIZE - 1000);
            assertThat(input.read()).isEqualTo(0);

            // seek back to the start of the file while only a few bytes remain in the open stream
            input.seek(0);
            assertThat(input.read()).isEqualTo(0);
        }

        assertThat(client.responses()).hasSize(2);
        TrackingStream first = client.responses().getFirst();
        // the remaining data is small, so the stream must be drained to allow connection reuse
        assertThat(first.aborted()).isFalse();
        assertThat(first.bytesRead()).isEqualTo(1000);
    }

    private static S3InputStream newInputStream(S3Client client)
    {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();
        return new S3InputStream(LOCATION, client, request, (long) FILE_SIZE);
    }

    private static final class FakeS3Client
            implements S3Client
    {
        private final byte[] data;
        private final List<TrackingStream> responses = new ArrayList<>();

        private FakeS3Client(byte[] data)
        {
            this.data = data;
        }

        public List<TrackingStream> responses()
        {
            return responses;
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request)
        {
            int start = 0;
            if (request.range() != null) {
                assertThat(request.range()).matches("bytes=\\d+-");
                start = toIntExact(Long.parseLong(request.range().substring("bytes=".length(), request.range().length() - 1)));
            }
            TrackingStream stream = new TrackingStream(new ByteArrayInputStream(data, start, data.length - start));
            responses.add(stream);
            GetObjectResponse response = GetObjectResponse.builder()
                    .contentLength((long) data.length - start)
                    .build();
            return new ResponseInputStream<>(response, AbortableInputStream.create(stream, stream::abort));
        }

        @Override
        public String serviceName()
        {
            return SERVICE_NAME;
        }

        @Override
        public void close() {}
    }

    private static final class TrackingStream
            extends FilterInputStream
    {
        private long bytesRead;
        private boolean aborted;

        private TrackingStream(InputStream in)
        {
            super(in);
        }

        public long bytesRead()
        {
            return bytesRead;
        }

        public boolean aborted()
        {
            return aborted;
        }

        public void abort()
        {
            aborted = true;
        }

        @Override
        public int read()
                throws IOException
        {
            int value = super.read();
            if (value >= 0) {
                bytesRead++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
                throws IOException
        {
            int n = super.read(bytes, offset, length);
            if (n > 0) {
                bytesRead += n;
            }
            return n;
        }

        @Override
        public long skip(long n)
                throws IOException
        {
            long skipped = super.skip(n);
            bytesRead += skipped;
            return skipped;
        }

        @Override
        public int available()
        {
            // a network stream only reports what is buffered locally
            return 0;
        }
    }
}
