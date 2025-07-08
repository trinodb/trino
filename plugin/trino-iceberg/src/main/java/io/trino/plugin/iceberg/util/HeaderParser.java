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
 package io.trino.plugin.iceberg.util;
 import java.util.HashMap;
 import java.util.Map;
 public final class HeaderParser
 {
     private HeaderParser() {}
     public static Map<String, String> parseHeaders(String headerString)
     {
         Map<String, String> headers = new HashMap<>();
         if (headerString == null || headerString.trim().isEmpty()) {
             return headers;
         }
         String[] pairs = headerString.split(",");
         for (String pair : pairs) {
             String[] keyValue = pair.split("=", 2);
             if (keyValue.length == 2) {
                 headers.put("header." + keyValue[0].trim(), keyValue[1].trim());
             }
         }
         return headers;
     }
 }
