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
package io.trino.web.ui;

import jakarta.ws.rs.NotFoundException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.web.ui.WebUiResources.mediaType;
import static io.trino.web.ui.WebUiResources.webUiResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestWebUiResources
{
    @Test
    public void testUiResources()
            throws IOException
    {
        assertThatThrownBy(() -> webUiResource("notExistingPath").getStatus())
                .isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> webUiResource("/webapp/index.html/../index.html").getStatus())
                .isInstanceOf(NotFoundException.class); // not canonical

        assertThat(webUiResource("/webapp/index.html").getStatus()).isEqualTo(200);
        assertThat(webUiResource("/webapp-preview/dist/index.html").getStatus()).isEqualTo(200);
    }

    @Test
    public void testMimetypes()
    {
        assertThat(mediaType("index.html")).isEqualTo("text/html");
        assertThat(mediaType("style.css")).isEqualTo("text/css");
        assertThat(mediaType("new.style.css")).isEqualTo("text/css");
        assertThat(mediaType("font.woff2")).isEqualTo("font/woff2");

        assertThat(mediaType("file.bat")).isEqualTo("application/octet-stream");
        assertThat(mediaType("filename")).isEqualTo("application/octet-stream");
        assertThat(mediaType("filename.")).isEqualTo("application/octet-stream");
    }
}
