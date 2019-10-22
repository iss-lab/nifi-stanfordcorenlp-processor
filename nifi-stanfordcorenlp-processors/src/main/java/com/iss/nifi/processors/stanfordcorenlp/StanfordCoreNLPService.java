package com.iss.nifi.processors.stanfordcorenlp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreEntityMention;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.StanfordCoreNLPClient;
import edu.stanford.nlp.pipeline.StanfordCoreNLPServer;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

public class StanfordCoreNLPService {
  private StanfordCoreNLPServer server;

  public StanfordCoreNLPService() throws IOException {
    this(true, null, 9000, 15000);
  }

  public StanfordCoreNLPService(boolean embeddedServer, Properties props, int port, int timeout) throws IOException {
    if (! embeddedServer) {
      return;
    }
    if (props == null) {
      props = new Properties();
    }
    server = new StanfordCoreNLPServer(props, port, timeout, false);
    server.run();
  }

  public StanfordCoreNLPClient getClient(Properties props) {
    if (props == null) {
      props = new Properties();
    }
    InetSocketAddress addr = server.getServer().get().getAddress();
    StanfordCoreNLPClient client = new StanfordCoreNLPClient(props, "http://" + addr.getHostString(), addr.getPort());
    return client;
  }

  public CoreDocument annotateDocument(String text, String annotators) {
    Properties props = new Properties();
    props.setProperty("annotators", annotators);

    CoreDocument document;

    if (server == null) {
      StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
      document = new CoreDocument(text);
      pipeline.annotate(document);
    } else {
      StanfordCoreNLPClient pipeline = getClient(props);
      Annotation annotation = pipeline.process(text);
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

    CoreDocument document = annotateDocument(text, "tokenize,ssplit,pos,lemma,ner");

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
  
  public static void main(String[] args) {
    String text = "Example Text.";
    
    try {
      StanfordCoreNLPService svc = new StanfordCoreNLPService();
      System.out.println(svc.extractEntities(text, "location,organization"));
      System.exit(0);
    } catch (Exception e) {
      System.out.println("Encountered exception while analyzing text: " + e);
    }
  }
}
