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
 * Signals that an {@code Grok} exception of some sort has occurred.
 * This class is the general class of
 * exceptions produced by failed or interrupted Grok operations.
 *
 * @author anthonycorbacho
 * @since 0.0.4
 */
// Note: this code is forked from oi.thekraken.grok.api.exception
// Copyright 2014 Anthony Corbacho, and contributors.
public class GrokException
        extends Exception
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new GrokException.
     */
    public GrokException()
    {
        super();
    }

    /**
     * Constructs a new GrokException.
     *
     * @param message the reason for the exception
     * @param cause   the underlying Throwable that caused this exception to be thrown.
     */
    public GrokException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Constructs a new GrokException.
     *
     * @param message the reason for the exception
     */
    public GrokException(String message)
    {
        super(message);
    }

    /**
     * Constructs a new GrokException.
     *
     * @param cause the underlying Throwable that caused this exception to be thrown.
     */
    public GrokException(Throwable cause)
    {
        super(cause);
    }
}
