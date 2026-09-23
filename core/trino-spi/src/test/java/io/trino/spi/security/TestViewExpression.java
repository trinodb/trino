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
package io.trino.spi.security;

import io.trino.spi.connector.CatalogSchemaName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TestViewExpression
{
    @Test
    public void testBuilderFrom()
    {
        ViewExpression originViewExpression = ViewExpression.builder()
                .identity("test_identity")
                .catalog("test_catalog")
                .schema("test_schema")
                .expression("test_expression")
                .path(List.of(new CatalogSchemaName("test_path_catalog", "test_path_schema")))
                .build();

        ViewExpression buildViewExpression = ViewExpression.builderFrom(originViewExpression).build();

        assertThat(buildViewExpression.getSecurityIdentity()).isEqualTo(Optional.of("test_identity"));
        assertThat(buildViewExpression.getCatalog()).isEqualTo(Optional.of("test_catalog"));
        assertThat(buildViewExpression.getSchema()).isEqualTo(Optional.of("test_schema"));
        assertThat(buildViewExpression.getExpression()).isEqualTo("test_expression");
        assertThat(buildViewExpression.getPath()).isEqualTo(List.of(new CatalogSchemaName("test_path_catalog", "test_path_schema")));
    }
}
