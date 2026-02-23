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
package io.trino.testing.containers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Dns;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Simple WebHDFS client for product tests.
 * <p>
 * Provides basic HDFS operations via the WebHDFS REST API.
 * <p>
 * This client supports custom DNS resolution to handle Docker container hostnames
 * that are not resolvable from the host machine. When WebHDFS returns redirects
 * to DataNodes using internal hostnames (like "hadoop-master"), the custom DNS
 * resolver maps these hostnames to the appropriate container IPs.
 */
public final class HdfsClient
{
    private static final MediaType OCTET_STREAM = MediaType.get("application/octet-stream");

    private final URI webHdfsUri;
    private final String username;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public HdfsClient(URI webHdfsUri, String username)
    {
        this(webHdfsUri, username, Map.of());
    }

    public HdfsClient(URI webHdfsUri, String username, Map<String, InetAddress> hostMapping)
    {
        this.webHdfsUri = requireNonNull(webHdfsUri, "webHdfsUri is null");
        this.username = requireNonNull(username, "username is null");
        requireNonNull(hostMapping, "hostMapping is null");
        this.httpClient = new OkHttpClient.Builder()
                .dns(hostname -> {
                    InetAddress address = hostMapping.get(hostname);
                    if (address != null) {
                        return List.of(address);
                    }
                    return Dns.SYSTEM.lookup(hostname);
                })
                .followRedirects(true)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Checks if a file or directory exists.
     */
    public boolean exist(String path)
    {
        URI uri = buildUri(path, "GETFILESTATUS");
        Request request = new Request.Builder()
                .url(uri.toString())
                .get()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            return response.code() == 200;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to check existence of " + path, e);
        }
    }

    /**
     * Creates a directory (and any necessary parent directories).
     */
    public void createDirectory(String path)
    {
        URI uri = buildUri(path, "MKDIRS");
        Request request = new Request.Builder()
                .url(uri.toString())
                .put(RequestBody.create(new byte[0]))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException("Failed to create directory " + path + ": " + response.body().string());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create directory " + path, e);
        }
    }

    /**
     * Creates a file with the given string content.
     */
    public void saveFile(String path, String content)
    {
        saveFile(path, content.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a file with the given binary content.
     */
    public void saveFile(String path, byte[] content)
    {
        // WebHDFS CREATE operation: PUT to NameNode returns 307 redirect to DataNode
        // OkHttpClient with followRedirects(true) handles this automatically
        URI uri = buildUri(path, "CREATE", "&overwrite=true");
        Request request = new Request.Builder()
                .url(uri.toString())
                .put(RequestBody.create(content, OCTET_STREAM))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 201) {
                throw new IOException("Failed to save file " + path + ": " + response.body().string());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to save file " + path, e);
        }
    }

    /**
     * Deletes a file or directory (recursively if directory).
     */
    public void delete(String path)
    {
        URI uri = buildUri(path, "DELETE", "&recursive=true");
        Request request = new Request.Builder()
                .url(uri.toString())
                .delete()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException("Failed to delete " + path + ": " + response.body().string());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete " + path, e);
        }
    }

    /**
     * Lists files in a directory.
     */
    public String[] listDirectory(String path)
    {
        URI uri = buildUri(path, "LISTSTATUS");
        Request request = new Request.Builder()
                .url(uri.toString())
                .get()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException("Failed to list directory " + path + ": " + response.body().string());
            }
            JsonNode root = objectMapper.readTree(response.body().string());
            JsonNode fileStatuses = root.path("FileStatuses").path("FileStatus");
            if (fileStatuses.isArray()) {
                String[] names = new String[fileStatuses.size()];
                for (int i = 0; i < fileStatuses.size(); i++) {
                    names[i] = fileStatuses.get(i).path("pathSuffix").asText();
                }
                return names;
            }
            return new String[0];
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to list directory " + path, e);
        }
    }

    /**
     * Returns the owner of a file or directory.
     */
    public String getFileOwner(String path)
    {
        return getFileStatus(path).path("owner").asText();
    }

    /**
     * Returns the group of a file or directory.
     */
    public String getFileGroup(String path)
    {
        return getFileStatus(path).path("group").asText();
    }

    /**
     * Reads the content of a file.
     */
    public byte[] readFile(String path)
    {
        // WebHDFS OPEN operation: GET to NameNode returns 307 redirect to DataNode
        URI uri = buildUri(path, "OPEN");
        Request request = new Request.Builder()
                .url(uri.toString())
                .get()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException("Failed to read file " + path + ": " + response.body().string());
            }
            return response.body().bytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read file " + path, e);
        }
    }

    private JsonNode getFileStatus(String path)
    {
        URI uri = buildUri(path, "GETFILESTATUS");
        Request request = new Request.Builder()
                .url(uri.toString())
                .get()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException("Failed to get file status for " + path + ": " + response.body().string());
            }
            JsonNode root = objectMapper.readTree(response.body().string());
            return root.path("FileStatus");
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to get file status for " + path, e);
        }
    }

    private URI buildUri(String path, String operation)
    {
        return buildUri(path, operation, "");
    }

    private URI buildUri(String path, String operation, String extraParams)
    {
        // Ensure path starts with /
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        String uriString = webHdfsUri + "/webhdfs/v1" + path + "?op=" + operation + "&user.name=" + username + extraParams;
        return URI.create(uriString);
    }
}
