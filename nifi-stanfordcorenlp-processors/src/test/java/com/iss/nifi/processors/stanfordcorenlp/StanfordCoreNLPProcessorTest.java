/*
 * 
 * MIT License
 *
 * Copyright (c) 2020 Institutional Shareholder Services. All other rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.iss.nifi.processors.stanfordcorenlp;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class StanfordCoreNLPProcessorTest {

    @Test
    public void testProcessor() {
        final TestRunner testRunner = TestRunners.newTestRunner(StanfordCoreNLPProcessor.class);
        testRunner.setThreadCount(1);

        testRunner.setProperty(StanfordCoreNLPProcessor.ENTITIES_PROPERTY, "location,organization");
        testRunner.setProperty(StanfordCoreNLPProcessor.PATH_PROPERTY, "$.['title','content']");
        // testRunner.setProperty(StanfordCoreNLPProcessor.PATH_PROPERTY, "$.content");

        try {
            testRunner.enqueue(new FileInputStream(new File("src/test/resources/test.json")));
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }

        testRunner.setValidateExpressionUsage(false);
        testRunner.run();
        testRunner.assertValid();
        final List<MockFlowFile> successFiles = testRunner
                .getFlowFilesForRelationship(StanfordCoreNLPProcessor.SUCCESS_RELATIONSHIP);

        for (final MockFlowFile mockFile : successFiles) {
            try {
                System.out.println("FILE:" + new String(mockFile.toByteArray(), "UTF-8"));
                final String attr = mockFile.getAttribute(StanfordCoreNLPProcessor.OUTPUT_ATTR);
                System.out.println("Attribute: " + attr);

                final Gson gson = new Gson();
                final Map<String, List<String>> attrJsonMap = gson.fromJson(attr, Map.class);
                assertTrue(attrJsonMap.containsKey("organization"));
                assertTrue(attrJsonMap.containsKey("location"));
                assertTrue(attrJsonMap.get("organization").size() == 2);
            } catch (final UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        testRunner.shutdown();
    }

    @Test
    public void testProcessorWithExternalServer() {
        final TestRunner testRunner = TestRunners.newTestRunner(StanfordCoreNLPProcessor.class);
        testRunner.setThreadCount(1);

        testRunner.setProperty(StanfordCoreNLPProcessor.ENTITIES_PROPERTY, "location,organization");
        testRunner.setProperty(StanfordCoreNLPProcessor.PATH_PROPERTY, "$.['title','content']");
        testRunner.setProperty(StanfordCoreNLPProcessor.HOST_PROPERTY, "http://localhost");
        testRunner.setProperty(StanfordCoreNLPProcessor.PORT_PROPERTY, "9000");

        try {
            testRunner.enqueue(new FileInputStream(new File("src/test/resources/test.json")));
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }

        testRunner.setValidateExpressionUsage(false);
        testRunner.run();
        testRunner.assertValid();
        final List<MockFlowFile> successFiles = testRunner
                .getFlowFilesForRelationship(StanfordCoreNLPProcessor.SUCCESS_RELATIONSHIP);

        for (final MockFlowFile mockFile : successFiles) {
            try {
                System.out.println("FILE:" + new String(mockFile.toByteArray(), "UTF-8"));
                final String attr = mockFile.getAttribute(StanfordCoreNLPProcessor.OUTPUT_ATTR);
                System.out.println("Attribute: " + attr);

                final Gson gson = new Gson();
                final Map<String, List<String>> attrJsonMap = gson.fromJson(attr, Map.class);
                assertTrue(attrJsonMap.containsKey("organization"));
                assertTrue(attrJsonMap.containsKey("location"));
                assertTrue(attrJsonMap.get("organization").size() == 2);
            } catch (final UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        testRunner.shutdown();
    }
}
