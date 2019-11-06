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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreEntityMention;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.StanfordCoreNLPClient;
import edu.stanford.nlp.util.PropertiesUtils;

public class StanfordCoreNLPService {
  private final static String DEFAULT_ANNOTATORS = "tokenize,ssplit,pos,lemma,ner";
  private final static int DEFAULT_THREADS = 1;

  private String host;
  private int port;
  private String apiKey;
  private String apiSecret;

  private Properties coreNLPProps;

  public StanfordCoreNLPService(Properties props) {
    this(props, null, 0, null, null);
  }

  public StanfordCoreNLPService(Properties props, String host, int port, String apiKey, String apiSecret) {
    coreNLPProps = sanitizeProps(props);
    this.host = host;
    this.port = port;
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
  }

  public CoreDocument annotateDocument(String text) {
    Annotation annotation = new Annotation(text);

    if (host == null) {
      StanfordCoreNLP pipeline = new StanfordCoreNLP(coreNLPProps);
      pipeline.annotate(annotation);
    } else {
      int threads = PropertiesUtils.getInt(coreNLPProps, "threads", DEFAULT_THREADS);
      StanfordCoreNLPClient pipeline = new StanfordCoreNLPClient(coreNLPProps, host, port, threads, apiKey, apiSecret);
      pipeline.annotate(annotation);
    }

    return new CoreDocument(annotation);
  }

  public Map<String, List<String>> extractEntities(String text, String entityTypes) {
    String[] entityTypeList = entityTypes.split((","));
    boolean extractLocations = false;
    List<String> locationNerTagList = new ArrayList<String>();
    locationNerTagList.add("LOCATION");
    locationNerTagList.add("CITY");
    locationNerTagList.add("COUNTRY");
    locationNerTagList.add("STATE_OR_PROVINCE");
    List<String> nerTagList = new ArrayList<String>();

    Map<String, List<String>> output = new HashMap<String, List<String>>();

    for (String tag : entityTypeList) {
      output.put(tag, new ArrayList<String>());
      if (tag.equals("location")) {
        extractLocations = true;
      } else {
        nerTagList.add(tag.toUpperCase());
      }
    }

    CoreDocument document = annotateDocument(text);
    try {
      List<CoreEntityMention> mentions = document.entityMentions();
      if (document.entityMentions() == null) {
        mentions = new ArrayList<CoreEntityMention>();
      }

      for (CoreEntityMention entityMention : mentions) {
        String eType = entityMention.entityType();
        if (extractLocations && locationNerTagList.contains(eType)) {
          List<String> locs = output.get("location");
          locs.add(entityMention.text());
          output.put("location", locs);
          continue;
        }
        if (nerTagList.contains(eType)) {
          List<String> e = output.get(eType.toLowerCase());
          e.add(entityMention.text());
          output.put(eType.toLowerCase(), e);
        }
      }

      return output;
    } catch (Exception e) {
      System.out.println("Exception while finding mentions: " + e);
    }
    return output;
  }

  public Properties sanitizeProps(Properties props) {
    if (props == null) {
      props = new Properties();
    }
    if (props.getProperty("annotators") == null) {
      props.setProperty("annotators", DEFAULT_ANNOTATORS);
    }
    Double threads = PropertiesUtils.getDouble(props, "threads", DEFAULT_THREADS);
    props.setProperty("threads", String.valueOf(threads.intValue()));

    return props;
  }
}
