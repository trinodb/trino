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
package io.trino.hive.formats.line.grok;

/**
 * {@code ResourceManager} .
 *
 * @author burak aydın.
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public final class ResourceManager
{
    private ResourceManager()
    {
    }

    public static final String PATTERNS = "src/main/resources/grok/patterns";
    public static final String DEFAULTDATATYPE = "src/main/resources/grok/datatype";
    public static final String DATEFORMAT = "src/main/resources/grok/dateformat";
    public static final String MESSAGES = "src/main/resources/grok/message/messages";
    public static final String ACCESS_LOG = "src/main/resources/grok/access_log";
    public static final String NASA = "src/main/resources/grok/nasa/";
    public static final String IP = "src/main/resources/grok/ip";
}
