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
package io.trino.plugin.thrift.api;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.thrift.api.NameValidationUtils.checkValidName;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNameValidationUtils
{
    @Test
    public void testCheckValidColumnName()
    {
        checkValidName("abc01_def2");
        assertThatThrownBy(() -> checkValidName(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("name is null or empty");
        assertThatThrownBy(() -> checkValidName(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("name is null or empty");
        assertThatThrownBy(() -> checkValidName("Abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must start with a lowercase latin letter");
        assertThatThrownBy(() -> checkValidName("0abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must start with a lowercase latin letter");
        assertThatThrownBy(() -> checkValidName("_abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must start with a lowercase latin letter");
        assertThatThrownBy(() -> checkValidName("aBc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must contain only lowercase latin letters, digits or underscores");
        assertThatThrownBy(() -> checkValidName("ab-c"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must contain only lowercase latin letters, digits or underscores");
    }
}
