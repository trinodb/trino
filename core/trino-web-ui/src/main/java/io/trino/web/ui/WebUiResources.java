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
package io.trino.web.ui;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static java.util.Locale.ENGLISH;

public class WebUiResources
{
    private static final String DEFAULT_MEDIA_TYPE = "application/octet-stream";

    private WebUiResources() {}

    public static Response webUiResource(String path)
            throws IOException
    {
        if (!isCanonical(path)) {
            // consider redirecting to the absolute path
            throw new NotFoundException();
        }

        try {
            URL resource = Resources.getResource(WebUiResources.class, path);
            return Response.ok(resource.openStream(), mediaType(resource.toString())).build();
        }
        catch (IllegalArgumentException e) {
            throw new NotFoundException();
        }
    }

    public static String readWebUiResource(String path)
            throws IOException
    {
        return Resources.toString(Resources.getResource(WebUiResources.class, path), StandardCharsets.UTF_8);
    }

    public static boolean isCanonical(String fullPath)
    {
        try {
            return new URI(fullPath).normalize().getPath().equals(fullPath);
        }
        catch (URISyntaxException e) {
            return false;
        }
    }

    @VisibleForTesting
    static String mediaType(String filename)
    {
        int dotPosition = filename.lastIndexOf('.');
        if (dotPosition == -1) {
            return DEFAULT_MEDIA_TYPE;
        }

        String extension = filename.substring(dotPosition + 1).toLowerCase(ENGLISH);
        return switch (extension) {
            case "apng" -> "image/apng";
            case "asc", "txt" -> "text/plain";
            case "bmp" -> "image/bmp";
            case "css" -> "text/css";
            case "html" -> "text/html";
            case "ico" -> "image/x-icon";
            case "gif" -> "image/gif";
            case "jpg", "jpeg" -> "image/jpeg";
            case "js" -> "application/javascript";
            case "json" -> "application/json";
            case "md", "markdown" -> "text/markdown";
            case "png" -> "image/png";
            case "svg" -> "image/svg+xml";
            case "toml" -> "application/toml";
            case "ttf" -> "font/ttf";
            case "webp" -> "image/webp";
            case "woff" -> "font/woff";
            case "woff2" -> "font/woff2";
            case "yaml", "yml" -> "application/yaml";
            default -> DEFAULT_MEDIA_TYPE;
        };
    }
}
