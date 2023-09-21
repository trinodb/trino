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
package io.trino.decoder.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.trino.spi.TrinoException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public interface DescriptorProvider
{
    Optional<Descriptor> getDescriptorFromTypeUrl(String url);

    default String getContents(String url)
    {
        requireNonNull(url, "url is null");
        ByteArrayOutputStream typeBytes = new ByteArrayOutputStream();
        try (InputStream stream = new URL(url).openStream()) {
            stream.transferTo(typeBytes);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_USER_ERROR, "Failed to read schema from URL", e);
        }
        return typeBytes.toString(UTF_8);
    }
}
