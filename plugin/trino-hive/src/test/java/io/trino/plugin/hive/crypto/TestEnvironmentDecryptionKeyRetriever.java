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
package io.trino.plugin.hive.crypto;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.testng.annotations.Test;

import java.util.Base64;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestEnvironmentDecryptionKeyRetriever
{
    private static final ColumnPath AGE = ColumnPath.fromDotString("age");

    @Test
    public void defaultEmpty()
    {
        EnvironmentDecryptionKeyRetriever retriever = new EnvironmentDecryptionKeyRetriever(null, null);

        assertThat(retriever.getFooterKey(Optional.empty())).isEmpty();
        assertThat(retriever.getColumnKey(AGE, Optional.empty())).isEmpty();
    }

    @Test
    public void singleKeyMode()
    {
        EnvironmentDecryptionKeyRetriever retriever = new EnvironmentDecryptionKeyRetriever(b64("foot"), b64("colKey"));

        assertThat(retriever.getFooterKey(Optional.of("ignored".getBytes(UTF_8)))).contains("foot".getBytes(UTF_8));
        assertThat(retriever.getColumnKey(AGE, Optional.empty())).contains("colKey".getBytes(UTF_8));
    }

    @Test
    public void mapModeUsesMetadata()
    {
        // footer: id1→k1 , id2→k2
        String footerValue = String.join(",",
                "id1:" + b64("k1"), "id2:" + b64("k2"));
        // column: meta→ageKey
        String columnValue = "meta:" + b64("ageKey");

        EnvironmentDecryptionKeyRetriever retriever = new EnvironmentDecryptionKeyRetriever(footerValue, columnValue);

        assertThat(retriever.getFooterKey(Optional.of("id2".getBytes(UTF_8)))).contains("k2".getBytes(UTF_8));
        assertThat(retriever.getColumnKey(AGE, Optional.of("meta".getBytes(UTF_8)))).contains("ageKey".getBytes(UTF_8));

        // unknown metadata → empty
        assertThat(retriever.getFooterKey(Optional.of("zzz".getBytes(UTF_8)))).isEmpty();
        assertThat(retriever.getColumnKey(AGE, Optional.empty())).isEmpty();
    }

    private static String b64(String string)
    {
        return Base64.getEncoder().encodeToString(string.getBytes(UTF_8));
    }
}
