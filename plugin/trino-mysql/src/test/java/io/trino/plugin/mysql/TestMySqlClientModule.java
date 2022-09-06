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
package io.trino.plugin.mysql;

import org.testng.annotations.Test;

import java.util.Properties;

import static io.trino.plugin.mysql.MySqlClientModule.validateConnectionUrl;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMySqlClientModule
{
    @Test
    public void testValidateConnectionUrl()
    {
        Properties properties = new Properties();
        properties.setProperty("characterEncoding", "utf8");
        properties.setProperty("character_set_server", "utf8mb4");
        assertThatThrownBy(() -> validateConnectionUrl("jdbc:mysql://localhost:3306?characterEncoding=big5", properties))
                .hasMessage("characterEncoding not allowed to be set in connectionUrl");
        assertThatThrownBy(() -> validateConnectionUrl("jdbc:mysql://localhost:3306?character_set_server=utf8mb4", properties))
                .hasMessage("character_set_server not allowed to be set in connectionUrl");
        assertThatThrownBy(() -> validateConnectionUrl("jdbc:mysql://localhost:3306?characterEncoding=big5&character_set_server=utf8mb4", properties))
                .hasMessage("characterEncoding, character_set_server not allowed to be set in connectionUrl");
        assertThatCode(() -> validateConnectionUrl("jdbc:mysql://localhost:3306", properties))
                .doesNotThrowAnyException();
        assertThatCode(() -> validateConnectionUrl("jdbc:mysql://localhost:3306?foo=bar", properties))
                .doesNotThrowAnyException();
    }
}
