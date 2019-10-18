package com.iss.nifi.processors.stanfordcorenlp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreEntityMention;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class StanfordCoreNLPService {
  public static final String[] locationKeys = "STATE_OR_PROVINCE,CITY".split(",");
  public static final String[] organizationKeys = "ORGANIZATION".split(",");

	public Map<String, List<String>> analyze(String text, String annotators, String format) {
    List<String> locations = new ArrayList<String>();
    List<String> organizations = new ArrayList<String>();

    Properties props = new Properties();
    props.setProperty("annotators", annotators);
    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    CoreDocument document = new CoreDocument(text);
    pipeline.annotate(document);
    
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
    StanfordCoreNLPService svc = new StanfordCoreNLPService();
    System.out.println(svc.analyze(text, "tokenize,ssplit,pos,lemma,ner,parse", ""));
  }
}
