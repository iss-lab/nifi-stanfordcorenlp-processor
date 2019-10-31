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
  private StanfordCoreNLPClient client;
  private Properties coreNLPProps;

  public StanfordCoreNLPService(Properties props) {
    if (props == null) {
      props = new Properties();
    }
    if (props.getProperty("annotators") == null) {
      props.setProperty("annotators", DEFAULT_ANNOTATORS);
    }
    coreNLPProps = props;
  }

  public StanfordCoreNLPService(
      Properties clientProps, 
      String host, 
      int port,
      String apiKey, 
      String apiSecret) {
    if (clientProps == null) {
      clientProps = new Properties();
    }
    int threads = PropertiesUtils.getInt(clientProps, "threads", 1);
    client = new StanfordCoreNLPClient(clientProps, host, port, threads, apiKey, apiSecret);
  }

  public CoreDocument annotateDocument(String text) {
    CoreDocument document;

    if (client == null) {
      StanfordCoreNLP pipeline = new StanfordCoreNLP(coreNLPProps);
      document = new CoreDocument(text);
      pipeline.annotate(document);
    } else {
      Annotation annotation = client.process(text);
      document = new CoreDocument(annotation);
    }

    return document;
  }

	public Map<String, List<String>> extractEntities(String text, String entityTypes)  {
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

    for (CoreEntityMention entityMention: document.entityMentions()) {
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
  }
}
