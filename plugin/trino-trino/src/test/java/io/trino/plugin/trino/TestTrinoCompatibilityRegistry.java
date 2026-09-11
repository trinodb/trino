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
package io.trino.plugin.trino;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.trino.TrinoRemoteCapabilities.CharToVarcharCastSemantics.TRIMS_TRAILING_SPACES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTrinoCompatibilityRegistry
{
    private final TrinoCompatibilityRegistry registry = new TrinoCompatibilityRegistry();
    private final TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.forTesting(Set.of("regexp_like", "count"));

    @Test
    void testAllowedFunction()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("regexp_like"), capabilities)).isTrue();
    }

    @Test
    void testDeniedFunction()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("remote_only_custom_function"), capabilities)).isFalse();
    }

    @Test
    void testAllowlistedFunctionRequiresRemoteMetadata()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("regexp_like"), TrinoRemoteCapabilities.forTesting(Set.of()))).isFalse();
    }

    @Test
    void testSessionSensitiveFunctionDenied()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("current_timestamp"), capabilities)).isFalse();
    }

    @Test
    void testLocaleSensitiveFunctionsDenied()
    {
        TrinoRemoteCapabilities localeFunctions = capabilitiesWithRemoteTimeZone(
                "UTC",
                "date_format",
                "date_parse",
                "format_datetime");

        assertThat(registry.isFunctionSupported(SESSION, call("date_format"), localeFunctions)).isFalse();
        assertThat(registry.isFunctionSupported(SESSION, call("date_parse"), localeFunctions)).isFalse();
        assertThat(registry.isFunctionSupported(SESSION, call("format_datetime"), localeFunctions)).isFalse();
    }

    @Test
    void testQualifiedFunctionDenied()
    {
        assertThat(registry.isFunctionSupported(
                SESSION,
                new Call(BOOLEAN, new FunctionName(Optional.of(new CatalogSchemaName("memory", "default")), "regexp_like"), List.of()),
                capabilities)).isFalse();
    }

    @Test
    void testStandardOperatorDoesNotRequireRemoteFunctionMetadata()
    {
        assertThat(registry.isFunctionSupported(
                SESSION,
                new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of()),
                TrinoRemoteCapabilities.forTesting(Set.of()))).isTrue();
    }

    @Test
    void testCastAddingSessionTimeZoneDeniedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, TIMESTAMP_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, DATE),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_TZ_MILLIS, TIME_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
    }

    @Test
    void testVarcharCastAddingSessionTimeZoneDeniedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, VARCHAR),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_TZ_MILLIS, VARCHAR),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
    }

    @Test
    void testCastUsingValueTimeZoneAllowedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_MILLIS, TIMESTAMP_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(DATE, TIMESTAMP_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_MILLIS, TIMESTAMP_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_MILLIS, TIME_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
    }

    @Test
    void testSessionSensitiveCastAllowedWhenRemoteTimeZoneMatches()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, TIMESTAMP_MILLIS),
                capabilitiesWithRemoteTimeZone("UTC"))).isTrue();
    }

    @Test
    void testCastDependingOnSessionStartAlwaysDenied()
    {
        TrinoRemoteCapabilities sameTimeZone = capabilitiesWithRemoteTimeZone("UTC");

        assertThat(registry.isFunctionSupported(utcSession(), cast(TIME_TZ_MILLIS, TIME_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_MILLIS, TIME_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_TZ_MILLIS, TIME_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_MILLIS, TIME_TZ_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_TZ_MILLIS, TIME_TZ_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIME_TZ_MILLIS, TIMESTAMP_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIME_TZ_MILLIS, VARCHAR), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_TZ_MILLIS, CharType.createCharType(20)),
                sameTimeZone)).isFalse();
    }

    @Test
    void testCharToTimestampWithTimeZoneRequiresMatchingTimeZone()
    {
        Type charTimestamp = CharType.createCharType(19);

        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, charTimestamp),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, charTimestamp),
                capabilitiesWithRemoteTimeZone("UTC"))).isTrue();
    }

    @Test
    void testTryCastUsesSameSessionDependencyRules()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                tryCast(TIMESTAMP_MILLIS, TIME_MILLIS),
                capabilitiesWithRemoteTimeZone("UTC"))).isFalse();
    }

    @Test
    void testUnknownTemporalCastDenied()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_MILLIS, DATE),
                capabilitiesWithRemoteTimeZone("UTC"))).isFalse();
    }

    @Test
    void testSessionSensitiveContainerCastDeniedRecursively()
    {
        TrinoRemoteCapabilities sameTimeZone = capabilitiesWithRemoteTimeZone("UTC");
        TrinoRemoteCapabilities differentTimeZone = capabilitiesWithRemoteTimeZone("Asia/Seoul");

        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(new ArrayType(TIMESTAMP_MILLIS), new ArrayType(TIME_MILLIS)),
                sameTimeZone)).isFalse();

        TypeOperators typeOperators = new TypeOperators();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(
                        new MapType(VARCHAR, TIMESTAMP_TZ_MILLIS, typeOperators),
                        new MapType(VARCHAR, TIMESTAMP_MILLIS, typeOperators)),
                differentTimeZone)).isFalse();

        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(
                        RowType.anonymous(List.of(VARCHAR, TIMESTAMP_TZ_MILLIS)),
                        RowType.anonymous(List.of(VARCHAR, TIMESTAMP_MILLIS))),
                sameTimeZone)).isTrue();
    }

    @Test
    void testNestedCharToVarcharCastRequiresRemoteTrimmingSemantics()
    {
        Type charType = CharType.createCharType(3);
        TypeOperators typeOperators = new TypeOperators();
        List<Call> casts = List.of(
                cast(new ArrayType(VARCHAR), new ArrayType(charType)),
                cast(
                        new MapType(BIGINT, VARCHAR, typeOperators),
                        new MapType(BIGINT, charType, typeOperators)),
                cast(
                        RowType.anonymous(List.of(BIGINT, VARCHAR)),
                        RowType.anonymous(List.of(BIGINT, charType))));

        for (Call cast : casts) {
            assertThat(registry.isFunctionSupported(SESSION, cast, capabilities))
                    .as("nested CHAR to VARCHAR cast with trimming semantics: %s", cast.getType())
                    .isTrue();
            assertThat(registry.isFunctionSupported(
                    SESSION,
                    cast,
                    TrinoRemoteCapabilities.forTestingLegacyCharToVarcharCast(Set.of())))
                    .as("nested CHAR to VARCHAR cast with retaining semantics: %s", cast.getType())
                    .isFalse();
            assertThat(registry.isFunctionSupported(SESSION, cast, TrinoRemoteCapabilities.unavailable()))
                    .as("nested CHAR to VARCHAR cast with unavailable semantics: %s", cast.getType())
                    .isFalse();
        }
    }

    @Test
    void testFromIso8601TimestampDeniedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                fromIso8601Timestamp(),
                capabilitiesWithRemoteTimeZone("Asia/Seoul", "from_iso8601_timestamp"))).isFalse();
    }

    @Test
    void testFromIso8601TimestampAllowedWhenRemoteTimeZoneMatches()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                fromIso8601Timestamp(),
                capabilitiesWithRemoteTimeZone("UTC", "from_iso8601_timestamp"))).isTrue();
    }

    private static Call call(String functionName)
    {
        return new Call(BOOLEAN, new FunctionName(functionName), List.of());
    }

    private static Call cast(Type targetType, Type sourceType)
    {
        return new Call(targetType, StandardFunctions.CAST_FUNCTION_NAME, List.of(new Constant(constantValue(sourceType), sourceType)));
    }

    private static Call tryCast(Type targetType, Type sourceType)
    {
        return new Call(targetType, StandardFunctions.TRY_CAST_FUNCTION_NAME, List.of(new Constant(constantValue(sourceType), sourceType)));
    }

    private static Call fromIso8601Timestamp()
    {
        return new Call(TIMESTAMP_TZ_MILLIS, new FunctionName("from_iso8601_timestamp"), List.of(new Constant(utf8Slice("2024-01-15T00:00:00"), VARCHAR)));
    }

    private static Object constantValue(Type type)
    {
        if (type instanceof VarcharType || type instanceof CharType) {
            return utf8Slice("2024-01-15T00:00:00");
        }
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return null;
        }
        return 0L;
    }

    private static ConnectorSession utcSession()
    {
        return TestingConnectorSession.builder()
                .setTimeZoneKey(getTimeZoneKey("UTC"))
                .build();
    }

    private static TrinoRemoteCapabilities capabilitiesWithRemoteTimeZone(String timeZone, String... functions)
    {
        return new TrinoRemoteCapabilities(
                Optional.of("477"),
                Optional.of(Set.of(functions)),
                Optional.of(timeZone),
                Optional.of(TRIMS_TRAILING_SPACES));
    }
}
