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
package io.trino.plugin.hive.metastore.glue;

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

import static java.util.Objects.requireNonNull;

public class ProxyApiRequestHandler
        implements ExecutionInterceptor
{
    private final String proxyApiId;

    public ProxyApiRequestHandler(String proxyApiId)
    {
        this.proxyApiId = requireNonNull(proxyApiId, "proxyApiId is null");
    }

    @Override
    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes)
    {
        // AWS Glue SDK will append "X-Amz-Target" header to requests (with "AWSGlue" prefix).
        // This misleads API Gateway (Glue proxy) that it's not the target of the REST call. Therefore, we
        // need to pass "X-Amz-Target" value in a special HTTP header that is translated back to "X-Amz-Target"
        // when API Gateway makes request to AWSGlue.
        SdkHttpRequest httpRequest = context.httpRequest();
        return httpRequest.toBuilder()
                .putHeader("x-apigw-api-id", proxyApiId)
                .putHeader("X-Trino-Amz-Target-Proxy", httpRequest.firstMatchingHeader("X-Amz-Target").orElse(null))
                .removeHeader("X-Amz-Target").build();
    }
}
