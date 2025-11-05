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
package io.trino.proxy;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;

public class AuthPreservingInterceptor
        implements Interceptor
{
    @Override
    public Response intercept(Interceptor.Chain chain)
            throws IOException
    {
        Request request = chain.request();
        Response response = chain.proceed(request);

        if (response.isRedirect()) {
            String authHeader = request.header(AUTHORIZATION);
            if (authHeader != null) {
                Request redirectedRequest = response.request().newBuilder()
                        .header(AUTHORIZATION, authHeader)
                        .build();
                return chain.proceed(redirectedRequest);
            }
        }
        return response;
    }
}
