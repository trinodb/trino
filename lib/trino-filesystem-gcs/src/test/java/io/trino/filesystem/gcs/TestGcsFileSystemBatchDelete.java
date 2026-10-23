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
package io.trino.filesystem.gcs;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestGcsFileSystemBatchDelete
{
    private static final Pattern DELETE_REQUEST_LINE = Pattern.compile("^DELETE \\S*/b/[^/]+/o/([^?\\s]+)", Pattern.MULTILINE);
    private static final String BOUNDARY = "batch_boundary";

    @Test
    void testDeleteFilesSucceedsForMissingFile()
            throws Exception
    {
        try (Storage storage = createStorage()) {
            fileSystem(storage).deleteFiles(List.of(Location.of("gs://bucket/deleted"), Location.of("gs://bucket/missing")));
        }
    }

    @Test
    void testDeleteFilesFailsOnPerItemError()
            throws Exception
    {
        try (Storage storage = createStorage()) {
            assertThatThrownBy(() -> fileSystem(storage).deleteFiles(List.of(Location.of("gs://bucket/deleted"), Location.of("gs://bucket/failing"))))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Failed to delete one or more files: {gs://bucket/failing=Internal Server Error (500)}");
        }
    }

    @Test
    void testDeleteDirectoryFailsOnPerItemError()
            throws Exception
    {
        try (Storage storage = createStorage()) {
            assertThatThrownBy(() -> fileSystem(storage).deleteDirectory(Location.of("gs://bucket/dir")))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Failed to delete one or more files: {gs://bucket/dir/failing=Internal Server Error (500)}");
        }
    }

    private static GcsFileSystem fileSystem(Storage storage)
    {
        return new GcsFileSystem(newDirectExecutorService(), storage, 1024, 1024, 100, 100, Optional.empty());
    }

    private static Storage createStorage()
    {
        MockHttpTransport transport = new MockHttpTransport()
        {
            @Override
            public LowLevelHttpRequest buildRequest(String method, String url)
            {
                if (method.equals("GET")) {
                    assertThat(url).contains("/b/bucket/o?").contains("prefix=dir/");
                    return new MockLowLevelHttpRequest(url)
                            .setResponse(new MockLowLevelHttpResponse()
                                    .setContentType("application/json")
                                    .setContent("{\"items\":[{\"bucket\":\"bucket\",\"name\":\"dir/deleted\"},{\"bucket\":\"bucket\",\"name\":\"dir/failing\"}]}"));
                }
                assertThat(url).endsWith("/batch/storage/v1");
                return new MockLowLevelHttpRequest(url)
                {
                    @Override
                    public LowLevelHttpResponse execute()
                            throws IOException
                    {
                        return batchResponse(getContentAsString());
                    }
                };
            }
        };
        return StorageOptions.newBuilder()
                .setProjectId("test")
                .setCredentials(NoCredentials.getInstance())
                .setRetrySettings(ServiceOptions.getNoRetrySettings())
                .setTransportOptions(HttpTransportOptions.newBuilder()
                        .setHttpTransportFactory(() -> transport)
                        .build())
                .build()
                .getService();
    }

    private static LowLevelHttpResponse batchResponse(String batchRequest)
    {
        StringBuilder content = new StringBuilder();
        Matcher matcher = DELETE_REQUEST_LINE.matcher(batchRequest);
        while (matcher.find()) {
            content.append("--").append(BOUNDARY).append("\r\n")
                    .append("Content-Type: application/http\r\n")
                    .append("\r\n")
                    .append(switch (URLDecoder.decode(matcher.group(1), UTF_8)) {
                        case "deleted", "dir/deleted" -> subResponse("204 No Content", "");
                        case "missing" -> errorSubResponse(404, "Not Found");
                        case "failing", "dir/failing" -> errorSubResponse(500, "Internal Server Error");
                        default -> throw new IllegalArgumentException("Unexpected object: " + matcher.group(1));
                    });
        }
        content.append("--").append(BOUNDARY).append("--\r\n");
        return new MockLowLevelHttpResponse()
                .setContentType("multipart/mixed; boundary=" + BOUNDARY)
                .setContent(content.toString());
    }

    private static String errorSubResponse(int code, String reason)
    {
        return subResponse(code + " " + reason, "{\"error\":{\"code\":%s,\"message\":\"%s\"}}".formatted(code, reason));
    }

    private static String subResponse(String status, String body)
    {
        return "HTTP/1.1 " + status + "\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "\r\n" +
                body + "\r\n";
    }
}
