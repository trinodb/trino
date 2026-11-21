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

import com.google.common.io.Resources;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ProtobufUtils
{
    private ProtobufUtils() {}

    public static FileDescriptor getFileDescriptor(String protoFile)
    {
        return getFileDescriptor(Optional.empty(), protoFile);
    }

    /**
     * Builds the descriptor of a {@code .proto} file whose imports, other than the well-known types, are resolved as classpath resources.
     */
    public static FileDescriptor getFileDescriptor(Optional<String> fileName, String protoFile)
    {
        return ProtoFileDescriptorBuilder.buildFileDescriptor(fileName.orElse(""), protoFile, ProtobufUtils::getClasspathProtoFile);
    }

    public static String getProtoFile(String filePath)
            throws IOException
    {
        return Resources.toString(Resources.getResource(ProtobufUtils.class, "/" + filePath), UTF_8);
    }

    private static Optional<String> getClasspathProtoFile(String filePath)
    {
        URL resource = ProtobufUtils.class.getResource("/" + filePath);
        if (resource == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Resources.toString(resource, UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
