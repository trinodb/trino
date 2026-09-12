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
package io.trino.lib;

import org.junit.jupiter.api.Test;
import org.weakref.solver.TypeSystem;
import org.weakref.solver.type.TypeConstructor;

import static org.assertj.core.api.Assertions.assertThat;

public class TrinoPresetTest
{
    @Test
    void testPresetRegistersAllTrinoTypeConstructors()
    {
        assertThat(TrinoPreset.typeConstructors())
                .extracting(TypeConstructor::name)
                .containsExactly(
                        "unknown",
                        "boolean",
                        "tinyint",
                        "smallint",
                        "integer",
                        "bigint",
                        "real",
                        "double",
                        "decimal",
                        "number",
                        "varchar",
                        "varchar",
                        "char",
                        "varbinary",
                        "date",
                        "time",
                        "time_with_time_zone",
                        "timestamp",
                        "timestamp_with_time_zone",
                        "interval_day_to_second",
                        "interval_year_to_month",
                        "json",
                        "uuid",
                        "ipaddress",
                        "variant",
                        "hyperloglog",
                        "p4hyperloglog",
                        "tdigest",
                        "qdigest",
                        "setdigest",
                        "color",
                        "json2016",
                        "map",
                        "array",
                        "row");
    }

    @Test
    void testPresetRegistersCoercionRules()
    {
        assertThat(TrinoPreset.coercionRules()).isNotEmpty();
    }

    @Test
    void testPresetTypeSystemExposesRegisteredContent()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        assertThat(typeSystem.types()).hasSize(TrinoPreset.typeConstructors().size());
        assertThat(typeSystem.coercions()).hasSize(TrinoPreset.coercionRules().size());
    }
}
