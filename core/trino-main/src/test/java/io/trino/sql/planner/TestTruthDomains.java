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
package io.trino.sql.planner;

import io.trino.spi.predicate.Domain;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTruthDomains
{
    @Test
    void testSqlTruthTables()
    {
        Boolean[] values = {true, false, null};
        Boolean[][] conjunction = {{true, false, null}, {false, false, false}, {null, false, null}};
        Boolean[][] disjunction = {{true, true, true}, {true, false, null}, {true, null, null}};
        Boolean[] negation = {false, true, null};
        for (int leftIndex = 0; leftIndex < values.length; leftIndex++) {
            Boolean left = values[leftIndex];
            TruthDomains leftDomains = domains(left);
            assertThat(result(leftDomains.not())).isEqualTo(negation[leftIndex]);
            for (int rightIndex = 0; rightIndex < values.length; rightIndex++) {
                Boolean right = values[rightIndex];
                assertThat(result(leftDomains.and(domains(right)))).describedAs("%s AND %s", left, right).isEqualTo(conjunction[leftIndex][rightIndex]);
                assertThat(result(leftDomains.or(domains(right)))).describedAs("%s OR %s", left, right).isEqualTo(disjunction[leftIndex][rightIndex]);
            }
        }
    }

    @Test
    void testValidation()
    {
        assertThatThrownBy(() -> new TruthDomains(Domain.all(BIGINT), Domain.all(BIGINT))).hasMessageContaining("overlap");
        assertThatThrownBy(() -> new TruthDomains(Domain.none(BIGINT), Domain.none(DATE))).hasMessageContaining("types differ");
    }

    private static TruthDomains domains(Boolean result)
    {
        return new TruthDomains(Boolean.TRUE.equals(result) ? Domain.all(BIGINT) : Domain.none(BIGINT), Boolean.FALSE.equals(result) ? Domain.all(BIGINT) : Domain.none(BIGINT));
    }

    private static Boolean result(TruthDomains domains)
    {
        if (domains.trueDomain().isAll()) {
            return true;
        }
        if (domains.falseDomain().isAll()) {
            return false;
        }
        return null;
    }
}
