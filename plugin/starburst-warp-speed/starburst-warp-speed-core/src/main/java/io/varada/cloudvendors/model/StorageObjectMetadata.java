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
package io.varada.cloudvendors.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public record StorageObjectMetadata(
        Map<String, String> userMetadata,
        Map<String, Object> metadata)
{
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String LAST_MODIFIED = "Last-Modified";

    public StorageObjectMetadata()
    {
        this(new HashMap<>(), new HashMap<>());
    }

    public Object getMetadata(String key)
    {
        return metadata.get(key);
    }

    public void putMetadata(String key, Object value)
    {
        metadata.put(key, value);
    }

    public Optional<Long> getContentLength()
    {
        return Optional.ofNullable((Long) getMetadata(CONTENT_LENGTH));
    }

    public void setContentLength(long contentLength)
    {
        putMetadata(CONTENT_LENGTH, contentLength);
    }

    public Optional<Long> getLastModified()
    {
        Long lastModified = (Long) getMetadata(LAST_MODIFIED);
        return lastModified == null ? Optional.empty() : Optional.of(lastModified);
    }

    public void setLastModified(long lastModified)
    {
        putMetadata(LAST_MODIFIED, lastModified);
    }
}
