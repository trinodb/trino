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
package io.trino.plugin.postgresql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringWriter;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/12/22 20:19
 */
public final class JacksonUtils
{
    private JacksonUtils()
    {
    }

    public static String deserialize(Object object)
    {
        try {
            StringWriter w = new StringWriter();
            ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.writeValue(w, object);
            return w.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
