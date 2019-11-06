/*
 * 
 * MIT License
 *
 * Copyright (c) 2019 Institutional Shareholder Services. All other rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.junit.Test;

public class StanfordCoreNLPServiceTest {

  @Test
  public void testExtractEntities() {
    StanfordCoreNLPService svc = new StanfordCoreNLPService(null);

    String text = "ALBUQUERQUE, N.M. — A worldwide film production company is expanding to Albuquerque, according to Albuquerque Business First. The company, Production Resource Group, has worked on various movie productions including \"House of Cards.\" They plan to move into a 6,000-square-foot warehouse space in northeast Albuquerque, located at 5821 Midway Park Blvd. NE. For more information, click here.";

    Map<String, List<String>> entities = svc.extractEntities(text, "location,organization");
    System.out.println("Entities: " + entities);

    assertNotNull(entities);
    assertEquals("Albuquerque Business First", entities.get("organization").get(0));

    svc = null;
  }

  @Test
  public void testExtractEntitiesWithServer() {
    StanfordCoreNLPService svc = new StanfordCoreNLPService(null, "http://localhost", 9000, null, null);

    String text = "ALBUQUERQUE, N.M. — A worldwide film production company is expanding to Albuquerque, according to Albuquerque Business First. The company, Production Resource Group, has worked on various movie productions including \"House of Cards.\" They plan to move into a 6,000-square-foot warehouse space in northeast Albuquerque, located at 5821 Midway Park Blvd. NE. For more information, click here.";

    Map<String, List<String>> entities = svc.extractEntities(text, "location,organization");
    System.out.println("Entities: " + entities);

    assertNotNull(entities);
    assertEquals("Albuquerque Business First", entities.get("organization").get(0));

    svc = null;
  }
}
