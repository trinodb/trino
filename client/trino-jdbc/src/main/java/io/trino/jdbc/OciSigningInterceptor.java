package io.trino.jdbc;

import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.http.signing.DefaultRequestSigner;
import com.oracle.bmc.http.signing.RequestSigner;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.Buffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OciSigningInterceptor
        implements Interceptor
{
    private final AuthenticationDetailsProvider authenticationDetailsProvider;

    public OciSigningInterceptor(String ociProfile, @Nullable String ociConfigPath)
    {
        Objects.requireNonNull(ociProfile, "ociProfile is null");
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
        RequestSigner signer = DefaultRequestSigner.create(authenticationDetailsProvider);

        Map<String, List<String>> convertedHeaders = convertHeaders(request);
        byte[] bodyBytes = bodyToBytes(request.body());

        try {
            // Ensure all necessary headers are present for signing, especially "host" and "date" if not already.
            // OkHttp typically adds Host. The OCI signer might add Date or expect it.
            // For Trino, common headers like X-Trino-User, X-Trino-Source, etc., will be part of `convertedHeaders`.

            Map<String, String> ociAuthHeaders = signer.signRequest(
                    request.url().uri(),
                    request.method(),
                    convertedHeaders,
                    bodyBytes);

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
