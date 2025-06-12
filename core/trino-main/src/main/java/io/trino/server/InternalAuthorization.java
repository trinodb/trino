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
package io.trino.server;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import jakarta.ws.rs.BadRequestException;

import javax.crypto.Mac;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record InternalAuthorization(String signature, String method, String uriPath, String nodeId, long ttl)
{
    private static final Splitter SPLITTER = Splitter.on(";").limit(3);
    private static final Joiner JOINER = Joiner.on(";");

    public InternalAuthorization
    {
        requireNonNull(signature, "signature is null");
        requireNonNull(method, "method is null");
        requireNonNull(method, "uriPath is null");
        requireNonNull(nodeId, "nodeId is null");
        checkArgument(ttl > 0, "ttl should be greater than zero");
    }

    public boolean isValid(Mac mac)
    {
        return signature.equals(calculateSignature(mac, method(), uriPath(), nodeId(), ttl()));
    }

    public static InternalAuthorization signRequest(Mac mac, String method, String uriPath, String nodeId, long ttl)
    {
        return new InternalAuthorization(calculateSignature(mac, method, uriPath, nodeId, ttl), method, uriPath, nodeId, ttl);
    }

    public String toHeader()
    {
        return JOINER.join(nodeId, Long.toString(ttl), signature);
    }

    public static InternalAuthorization fromHeader(String header, String method, String uriPath)
    {
        List<String> parts = SPLITTER.splitToList(header);
        if (parts.size() != 3) {
            throw new BadRequestException("Invalid signature header format");
        }

        return new InternalAuthorization(
                parts.getLast(),
                method,
                uriPath,
                parts.getFirst(),
                Long.parseLong(parts.get(1)));
    }

    private static byte normalizeMethod(String method)
    {
        return switch (method.toUpperCase(ENGLISH)) {
            case "GET" -> 0x01;
            case "POST" -> 0x02;
            case "PUT" -> 0x03;
            case "DELETE" -> 0x04;
            case "HEAD" -> 0x05;
            case "OPTIONS" -> 0x06;
            case "PATCH" -> 0x07;
            default -> 0x00;
        };
    }

    private static String calculateSignature(Mac mac, String method, String uriPath, String nodeId, long ttl)
    {
        Slice slice = Slices.allocate(nodeId.length() + uriPath.length() + 9);
        // Sum of strings length + byte + long
        try (SliceOutput output = slice.getOutput()) {
            output.appendBytes(nodeId.getBytes(UTF_8));
            output.appendBytes(uriPath.getBytes(UTF_8));
            output.appendByte(normalizeMethod(method));
            output.appendLong(ttl);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getEncoder().encodeToString(mac.doFinal(slice.byteArray()));
    }
}
