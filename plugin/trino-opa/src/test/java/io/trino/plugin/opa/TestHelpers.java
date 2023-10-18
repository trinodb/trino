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
package io.trino.plugin.opa;

import com.google.common.collect.Sets;
import io.trino.execution.QueryIdGenerator;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;

import java.time.Instant;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class TestHelpers
{
    private TestHelpers() {}

    public static final HttpClientUtils.MockResponse OK_RESPONSE = new HttpClientUtils.MockResponse("""
            {
                "decision_id": "",
                "result": true
            }
            """,
            200);
    public static final HttpClientUtils.MockResponse NO_ACCESS_RESPONSE = new HttpClientUtils.MockResponse("""
            {
                "decision_id": "",
                "result": false
            }
            """,
            200);
    public static final HttpClientUtils.MockResponse MALFORMED_RESPONSE = new HttpClientUtils.MockResponse("""
            { "this"": is broken_json; }
            """,
            200);
    public static final HttpClientUtils.MockResponse UNDEFINED_RESPONSE = new HttpClientUtils.MockResponse("{}", 404);
    public static final HttpClientUtils.MockResponse BAD_REQUEST_RESPONSE = new HttpClientUtils.MockResponse("{}", 400);
    public static final HttpClientUtils.MockResponse SERVER_ERROR_RESPONSE = new HttpClientUtils.MockResponse("", 500);

    public static Stream<Arguments> createFailingTestCases(Stream<Arguments> baseTestCases)
    {
        return Sets.cartesianProduct(
                        baseTestCases.collect(toImmutableSet()),
                        allErrorCasesArgumentProvider().collect(toImmutableSet()))
                .stream()
                .map(items -> Arguments.of(items.stream().flatMap((args) -> Arrays.stream(args.get())).toArray()));
    }

    public static Stream<Arguments> createIllegalResponseTestCases(Stream<Arguments> baseTestCases)
    {
        return Sets.cartesianProduct(
                        baseTestCases.collect(toImmutableSet()),
                        illegalResponseArgumentProvider().collect(toImmutableSet()))
                .stream()
                .map(items -> Arguments.of(items.stream().flatMap((args) -> Arrays.stream(args.get())).toArray()));
    }

    public static Stream<Arguments> illegalResponseArgumentProvider()
    {
        // Invalid responses from OPA
        return Stream.of(
                Arguments.of(Named.of("Undefined policy response", UNDEFINED_RESPONSE), OpaQueryException.OpaServerError.PolicyNotFound.class, "did not return a value"),
                Arguments.of(Named.of("Bad request response", BAD_REQUEST_RESPONSE), OpaQueryException.OpaServerError.class, "returned status 400"),
                Arguments.of(Named.of("Server error response", SERVER_ERROR_RESPONSE), OpaQueryException.OpaServerError.class, "returned status 500"),
                Arguments.of(Named.of("Malformed JSON response", MALFORMED_RESPONSE), OpaQueryException.class, "Failed to deserialize"));
    }

    public static Stream<Arguments> allErrorCasesArgumentProvider()
    {
        // All possible failure scenarios, including a well-formed access denied response
        return Stream.concat(
                illegalResponseArgumentProvider(),
                Stream.of(Arguments.of(Named.of("No access response", NO_ACCESS_RESPONSE), AccessDeniedException.class, "Access Denied")));
    }

    public static SystemSecurityContext systemSecurityContextFromIdentity(Identity identity) {
        return new SystemSecurityContext(identity, new QueryIdGenerator().createNextQueryId(), Instant.now());
    }

    public static <T> BiConsumer<OpaAccessControl, SystemSecurityContext> convertSystemSecurityContextToIdentityArgument(
            BiConsumer<OpaAccessControl, Identity> callable)
    {
        return (accessControl, systemSecurityContext) -> callable.accept(accessControl, systemSecurityContext.getIdentity());
    }

    public static <T> FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, T> convertSystemSecurityContextToIdentityArgument(
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, T> callable) {
        return (accessControl, systemSecurityContext, argument) -> callable.accept(accessControl, systemSecurityContext.getIdentity(), argument);
    }
}
