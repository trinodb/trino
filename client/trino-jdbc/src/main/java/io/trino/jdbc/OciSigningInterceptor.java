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
package io.trino.jdbc;

import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.http.client.io.DuplicatableInputStream;
import com.oracle.bmc.http.signing.DefaultRequestSigner;
import com.oracle.bmc.http.signing.RequestSigner;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.Buffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OciSigningInterceptor
        implements Interceptor
{
    private final AuthenticationDetailsProvider authenticationDetailsProvider;

    public OciSigningInterceptor(String ociProfile, @Nullable String ociConfigPath)
    {
        try {
            // ConfigFileAuthenticationDetailsProvider correctly handles null ociConfigPath
            // by defaulting to "~/.oci/config" or environment variables.
            this.authenticationDetailsProvider = new ConfigFileAuthenticationDetailsProvider(ociConfigPath, ociProfile);
        }
        catch (IOException e) {
            // It's generally better to have a specific exception, but for now,
            // RuntimeException is acceptable as per the instructions.
            throw new RuntimeException(
                    "Failed to load OCI configuration for profile '" + ociProfile + "'" +
                            (ociConfigPath != null ? " from path '" + ociConfigPath + "'" : " (using default configuration path or environment variables)") +
                            ": " + e.getMessage(),
                    e);
        }
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        Request request = chain.request();
        RequestSigner signer = DefaultRequestSigner.createRequestSigner(authenticationDetailsProvider);

        Map<String, List<String>> convertedHeaders = convertHeaders(request);
        byte[] bodyBytes = bodyToBytes(request.body());

        try {
            // Ensure all necessary headers are present for signing, especially "host" and "date" if not already.
            // OkHttp typically adds Host. The OCI signer might add Date or expect it.
            // For Trino, common headers like X-Trino-User, X-Trino-Source, etc., will be part of `convertedHeaders`.

            DuplicatableInputStream duplicatableBody = null;
            if (bodyBytes != null) {
                duplicatableBody = new SimpleDuplicatableInputStream(bodyBytes);
            }

            Map<String, String> ociAuthHeaders = signer.signRequest(
                    request.url().uri(),
                    request.method(),
                    convertedHeaders,
                    duplicatableBody);

            Request.Builder signedRequestBuilder = request.newBuilder();
            for (Map.Entry<String, String> entry : ociAuthHeaders.entrySet()) {
                // The OCI SDK might return headers that OkHttp already manages (like Host).
                // Adding them again can cause issues. OkHttp's Request.Builder.header()
                // replaces existing headers, which is generally the desired behavior here.
                signedRequestBuilder.header(entry.getKey(), entry.getValue());
            }
            return chain.proceed(signedRequestBuilder.build());
        }
        catch (Exception e) {
            // Wrapping OCI SDK specific exceptions into IOException as expected by OkHttp Interceptors
            throw new IOException("Failed to sign request using OCI credentials for profile: " +
                    authenticationDetailsProvider.getKeyId() + // Provides context like tenant/user/fingerprint
                    ". Error: " + e.getMessage(), e);
        }
    }

    private Map<String, List<String>> convertHeaders(Request request)
    {
        Map<String, List<String>> map = new HashMap<>();
        for (String name : request.headers().names()) {
            // OkHttp's headers(name) returns a List<String>
            map.put(name, request.headers(name));
        }
        return map;
    }

    private byte[] bodyToBytes(@Nullable RequestBody body)
            throws IOException
    {
        if (body == null) {
            return null;
        }
        Buffer buffer = new Buffer();
        body.writeTo(buffer);
        return buffer.readByteArray();
    }
}
