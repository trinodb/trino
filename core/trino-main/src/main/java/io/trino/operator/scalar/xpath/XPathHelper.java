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
package io.trino.operator.scalar.xpath;

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.TrinoException;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static javax.xml.xpath.XPathConstants.BOOLEAN;
import static javax.xml.xpath.XPathConstants.NODE;
import static javax.xml.xpath.XPathConstants.NODESET;
import static javax.xml.xpath.XPathConstants.NUMBER;
import static javax.xml.xpath.XPathConstants.STRING;

/**
 * Utility class for XML processing; borrowed from Hive UDFXPathUtil
 * https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/xml/UDFXPathUtil.java
 */
final class XPathHelper
{
    public static final String SAX_FEATURE_PREFIX = "http://xml.org/sax/features/";
    public static final String EXTERNAL_GENERAL_ENTITIES_FEATURE = "external-general-entities";
    public static final String EXTERNAL_PARAMETER_ENTITIES_FEATURE = "external-parameter-entities";

    private DocumentBuilderFactory documentBuilderFactory;
    private DocumentBuilder documentBuilder;
    private ReusableStringReader reader;
    private InputSource inputSource;
    private XPath xpath;
    private XPathExpression expression;
    private String oldPath;

    public XPathHelper()
    {
        this.documentBuilderFactory = DocumentBuilderFactory.newInstance();
        this.reader = new ReusableStringReader();
        this.inputSource = new InputSource(reader);
        this.xpath = XPathFactory.newInstance().newXPath();
    }

    public Object eval(String xml, String path, QName qname)
    {
        if (xml == null || path == null || qname == null) {
            return null;
        }
        if (xml.length() == 0 || path.length() == 0) {
            return null;
        }
        if (!path.equals(oldPath)) {
            try {
                expression = xpath.compile(path);
            }
            catch (XPathExpressionException e) {
                expression = null;
            }
            oldPath = path;
        }
        if (expression == null) {
            return null;
        }
        if (documentBuilder == null) {
            try {
                documentBuilderFactory.setFeature(SAX_FEATURE_PREFIX + EXTERNAL_GENERAL_ENTITIES_FEATURE, false);
                documentBuilderFactory.setFeature(SAX_FEATURE_PREFIX + EXTERNAL_PARAMETER_ENTITIES_FEATURE, false);
                documentBuilder = documentBuilderFactory.newDocumentBuilder();
            }
            catch (ParserConfigurationException e) {
                throw new RuntimeException("Error instantiating DocumentBuilder; cannot build XML parser", e);
            }
        }

        reader.set(xml);
        try {
            return expression.evaluate(documentBuilder.parse(inputSource), qname);
        }
        catch (XPathExpressionException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid expression '%s'", oldPath), e);
        }
        catch (SAXParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Error parsing xml data '%s'", xml), e);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Error evaluating expression '%s'", oldPath), e);
        }
    }

    public Boolean evalBoolean(String xml, String path)
    {
        return (Boolean) eval(xml, path, BOOLEAN);
    }

    public String evalString(String xml, String path)
    {
        return (String) eval(xml, path, STRING);
    }

    public Double evalNumber(String xml, String path)
    {
        return (Double) eval(xml, path, NUMBER);
    }

    public Node evalNode(String xml, String path)
    {
        return (Node) eval(xml, path, NODE);
    }

    public Optional<NodeList> evalNodeList(String xml, String path)
    {
        return Optional.ofNullable((NodeList) eval(xml, path, NODESET));
    }

    /**
     * Reusable, threadsafe version of {@link StringReader}.
     */
    @ThreadSafe
    private static final class ReusableStringReader
            extends Reader
    {
        private String streamData;
        private int length = -1;
        private int next;
        private int mark;

        public void set(String streamData)
        {
            this.streamData = streamData;
            this.length = streamData.length();
            this.next = 0;
            this.mark = 0;
        }

        /** Check to make sure that the stream has not been closed */
        private void ensureOpen()
                throws IOException
        {
            if (streamData == null) {
                throw new IOException("Stream is closed");
            }
        }

        @Override
        public int read()
                throws IOException
        {
            ensureOpen();
            if (next >= length) {
                return -1;
            }
            return streamData.charAt(next++);
        }

        @Override
        public int read(char[] cbuf, int off, int len)
                throws IOException
        {
            ensureOpen();
            if (off < 0 || off > cbuf.length || len < 0 || (off + len) > cbuf.length || (off + length) < 0) {
                throw new IndexOutOfBoundsException();
            }
            else if (len == 0) {
                return 0;
            }

            if (next >= length) {
                return -1;
            }

            int n = Math.min(length - next, len);
            streamData.getChars(next, next + n, cbuf, off);
            next += n;
            return n;
        }

        @Override
        public long skip(long ns)
                throws IOException
        {
            ensureOpen();
            if (next >= length) {
                return 0;
            }
            // Bound skip by beginning and end of the source
            int n = Long.valueOf(Math.min(length - next, ns)).intValue();
            n = Math.max(-next, n);
            next += n;
            return n;
        }

        @Override
        public boolean ready()
                throws IOException
        {
            ensureOpen();
            return true;
        }

        @Override
        public boolean markSupported()
        {
            return true;
        }

        @Override
        public void mark(int readAheadLimit)
                throws IOException
        {
            if (readAheadLimit < 0) {
                throw new IllegalArgumentException("Read-ahead limit < 0");
            }
            ensureOpen();
            mark = next;
        }

        @Override
        public void reset()
                throws IOException
        {
            ensureOpen();
            next = mark;
        }

        @Override
        public void close()
        {
            streamData = null;
        }
    }
}
