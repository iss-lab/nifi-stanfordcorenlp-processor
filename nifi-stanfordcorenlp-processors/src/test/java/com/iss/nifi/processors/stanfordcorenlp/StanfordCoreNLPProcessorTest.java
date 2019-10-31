/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iss.nifi.processors.stanfordcorenlp;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StanfordCoreNLPProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(StanfordCoreNLPProcessor.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty(StanfordCoreNLPProcessor.ENTITIES_PROPERTY, "location,organization");
        testRunner.setProperty(StanfordCoreNLPProcessor.PATH_PROPERTY, "$.['title','content']");
        // testRunner.setProperty(StanfordCoreNLPProcessor.PATH_PROPERTY, "$.content");
		
		try {
			testRunner.enqueue(new FileInputStream(new File("src/test/resources/test.json")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
        }
        
        testRunner.setValidateExpressionUsage(false);
		testRunner.run();
		testRunner.assertValid();
		List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(StanfordCoreNLPProcessor.SUCCESS_RELATIONSHIP);

		for (MockFlowFile mockFile : successFiles) {
			try {
                System.out.println("FILE:" + new String(mockFile.toByteArray(), "UTF-8"));
                String attr = mockFile.getAttribute(StanfordCoreNLPProcessor.OUTPUT_ATTR);
                System.out.println("Attribute: " + attr);
                assertNotNull( attr );
                String output = mockFile.toString();
				System.out.println("Output: " + output);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
    }

    @After
    public void teardown() {
        // TODO: Teardown server if running
    }

}
