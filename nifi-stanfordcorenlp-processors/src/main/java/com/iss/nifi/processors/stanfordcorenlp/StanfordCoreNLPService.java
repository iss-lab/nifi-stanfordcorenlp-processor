package com.iss.nifi.processors.stanfordcorenlp;

import java.io.IOException;
import java.net.InetSocketAddress;
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
import edu.stanford.nlp.pipeline.StanfordCoreNLPServer;

public class StanfordCoreNLPService {
  public static final String[] locationKeys = "STATE_OR_PROVINCE,CITY".split(",");
  public static final String[] organizationKeys = "ORGANIZATION".split(",");

  private StanfordCoreNLPServer server;

  public StanfordCoreNLPService() throws IOException {
    this(null, true);
  }

  public StanfordCoreNLPService(Properties props, boolean embeddedServer) throws IOException {
    if (! embeddedServer) {
      return;
    }
    if (props == null) {
      props = new Properties();
    }
    server = new StanfordCoreNLPServer(props);
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

	public Map<String, List<String>> analyze(String text, String annotators)  {
    List<String> locations = new ArrayList<String>();
    List<String> organizations = new ArrayList<String>();

    Properties props = new Properties();
    props.setProperty("annotators", annotators);

    // StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    // CoreDocument document = new CoreDocument(text);
    // pipeline.annotate(document);

    StanfordCoreNLPClient pipeline = getClient(props);
    Annotation annotation = pipeline.process(text);
    System.out.println("Creating core doc");
    CoreDocument document = new CoreDocument(annotation);

    for (CoreEntityMention entityMention: document.entityMentions()) {
      for (String k : locationKeys) {
        if (k.equals(entityMention.entityType())) {
          locations.add(entityMention.text());
        }
      }
      for (String k : organizationKeys) {
        if (k.equals(entityMention.entityType())) {
          organizations.add(entityMention.text());
        }
      }
    }

    Map<String, List<String>> output = new HashMap<String, List<String>>();
    output.put("organizations", organizations);
    output.put("locations", locations);

    return output;
  }
  
  public static void main(String[] args) {
    String text = "Example Text.";
    System.out.println("Analyzing text: " + text);
    try {
      StanfordCoreNLPService svc = new StanfordCoreNLPService();
      System.out.println(svc.analyze(text, "tokenize,ssplit,pos,lemma,ner"));
      System.exit(0);
    } catch (Exception e) {
      System.out.println("Encountered exception while analyzing text: " + e);
    }
  }
}
