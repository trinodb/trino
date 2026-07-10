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
package io.trino.plugin.vdm.utils;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.spi.connector.ConnectorViewDefinition;

import java.util.Base64;

/**
 * ViewUtil
 *
 * @since 2023-04-06
 */
public class VdmUtil
{
    /**
     * hetu version
     */
    public static final String HETU_VERSION_NAME = "hetu_version";
    /**
     * hetu view flag
     */
    public static final String HETU_VIEW_FLAG = "hetu_view";
    /**
     * hetu query id
     */
    public static final String HETU_QUERY_ID_NAME = "hetu_query_id";
    private static final String VIEW_PREFIX = "/* Hetu View: ";
    private static final String VIEW_SUFFIX = " */";
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC =
            new JsonCodecFactory().jsonCodec(ConnectorViewDefinition.class);

    private VdmUtil()
    {
    }

    /**
     * encode sql
     *
     * @param definition definition
     * @return encode sql
     */
    public static String encodeViewData(ConnectorViewDefinition definition)
    {
        byte[] bytes = VIEW_CODEC.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return VIEW_PREFIX + data + VIEW_SUFFIX;
    }

    /**
     * decode sql
     *
     * @param data base64 sql
     * @return sql
     */
    public static ConnectorViewDefinition decodeViewData(String data)
    {
        String sql;
        sql = data.substring(VIEW_PREFIX.length());
        sql = sql.substring(0, sql.length() - VIEW_SUFFIX.length());
        return VIEW_CODEC.fromJson(Base64.getDecoder().decode(sql));
    }

    /**
     * check is hetu view
     *
     * @param flag flag
     * @return is hetu view
     */
    public static boolean isHetuView(String flag)
    {
        return "true".equals(flag);
    }
}
