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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestOracleLegacyVarcharToCharCoercionCastPushdown
        extends BaseOracleCastPushdownTest
{
    // With the deprecated varchar-to-char coercion direction the engine emits the char -> varchar cast as
    // $legacy_char_to_varchar_cast, which re-pads the value to the char type length in code points. Oracle's
    // CAST(CHAR AS VARCHAR2) instead keeps the column's blank padding in bytes, so for multibyte data the two
    // disagree. The cast is therefore not pushed down and the engine evaluates it.
    private static final List<CastTestCase> LEGACY_CHAR_TO_VARCHAR_CASTS = ImmutableList.<CastTestCase>builder()
            .add(new CastTestCase("c_char_10", "varchar(50)", "c_varchar_50"))
            .add(new CastTestCase("c_char_50", "varchar(50)", "c_varchar_50"))
            .add(new CastTestCase("c_char_501", "varchar(50)", "c_varchar_50"))
            .add(new CastTestCase("c_char_520", "varchar(50)", "c_varchar_50"))
            .add(new CastTestCase("c_nchar_10", "varchar(50)", "c_varchar_50"))
            .add(new CastTestCase("c_char_unicode", "varchar(50)", "c_varchar_50"))
            .add(new CastTestCase("c_nchar_unicode", "varchar(50)", "c_varchar_50"))
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(new TestingOracleServer());
        return OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("jdbc-types-mapped-to-varchar", "interval year(2) to month, timestamp(6) with local time zone")
                        .put("join-pushdown.enabled", "true")
                        .buildOrThrow())
                .addExtraProperty("deprecated.legacy-varchar-to-char-coercion", "true")
                .build();
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return super.supportedCastTypePushdown().stream()
                .filter(testCase -> !LEGACY_CHAR_TO_VARCHAR_CASTS.contains(testCase))
                .collect(toImmutableList());
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .addAll(super.unsupportedCastTypePushdown())
                .addAll(LEGACY_CHAR_TO_VARCHAR_CASTS)
                .build();
    }
}
