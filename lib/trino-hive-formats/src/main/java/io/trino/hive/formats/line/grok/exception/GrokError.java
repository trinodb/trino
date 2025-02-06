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
package io.trino.hive.formats.line.grok.exception;

/**
 * Not used, too much c style..
 *
 * @author anthonycorbacho
 *
 */
// Note: this code is forked from oi.thekraken.grok.api.exception
// Copyright 2014 Anthony Corbacho, and contributors.
@Deprecated
public class GrokError
{
    private GrokError() {}

    public static final int GROK_OK = 0;
    public static final int GROK_ERROR_FILE_NOT_ACCESSIBLE = 1;
    public static final int GROK_ERROR_PATTERN_NOT_FOUND = 2;
    public static final int GROK_ERROR_UNEXPECTED_READ_SIZE = 3;
    public static final int GROK_ERROR_COMPILE_FAILED = 4;
    public static final int GROK_ERROR_UNINITIALIZED = 5;
    public static final int GROK_ERROR_NOMATCH = 6;
}
