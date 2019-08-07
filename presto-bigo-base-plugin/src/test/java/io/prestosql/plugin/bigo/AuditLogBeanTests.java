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
package io.prestosql.plugin.bigo;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;

/**
 * @author tangyun@bigo.sg
 * @date 7/2/19 2:37 PM
 */
public class AuditLogBeanTests
{
    public static class TestBean {
        private String thisIsOk = "sdkhjlflskdf";
        @Override
        public String toString() {

            JSONObject jsonObject = new JSONObject();
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field: fields) {
                String name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
                field.setAccessible(true);
                Object value = null;
                try {
                    value = field.get(this);
                    jsonObject.put(name, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            return jsonObject.toString();
        }
    }

    @Test
    public void test01()
    {
        String instant = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("Asia/Shanghai")).toString();
        System.out.println(instant);
        System.out.println(new Date().toInstant());
    }
    @Test
    public void test02()
    {
        TestBean auditLogBeanTests = new TestBean();
        System.out.println(auditLogBeanTests);
    }

    @Test
    public void test03() {
        System.out.println(CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, "test-data"));//testData
        System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "test_data"));//testData
        System.out.println(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, "test_data"));//TestData

        System.out.println(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, "testdata"));//testdata
        System.out.println(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, "TestData"));//test_data
        System.out.println(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, "testData"));//test-data

    }
}
