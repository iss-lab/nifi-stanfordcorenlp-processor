package com.iss.nifi.processors.stanfordcorenlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.pipeline.ProtobufAnnotationSerializer;
import edu.stanford.nlp.util.StringUtils;
import edu.stanford.nlp.util.logging.Redwood;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * An annotation pipeline in spirit identical to {@link StanfordCoreNLP}, but
 * with the backend supported by a web server.
 *
 * @author Gabor Angeli
 */
@SuppressWarnings("FieldCanBeLocal")
public class StanfordCoreNLPClientSimple extends AnnotationPipeline  {

  /** A logger for this class */
  private static final Redwood.RedwoodChannels log = Redwood.channels(StanfordCoreNLPClientSimple.class);

  /**
   * Information on how to connect to a backend.
   * The semantics of one of these objects is as follows:
   *
   * <ul>
   *   <li>It should define a hostname and port to connect to.</li>
   *   <li>This represents ONE thread on the remote server. The client should
   *       treat it as such.</li>
   *   <li>Two backends that are .equals() point to the same endpoint, but there can be
   *       multiple of them if we want to run multiple threads on that endpoint.</li>
   * </ul>
   */
  private static class Backend {
    /** The protocol to connect to the server with. */
    public final String protocol;
    /** The hostname of the server running the CoreNLP annotators */
    public final String host;
    /** The port of the server running the CoreNLP annotators */
    public final int port;
    public Backend(String protocol, String host, int port) {
      this.protocol = protocol;
      this.host = host;
      this.port = port;
    }
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Backend)) return false;
      Backend backend = (Backend) o;
      return port == backend.port && protocol.equals(backend.protocol) && host.equals(backend.host);
    }
    @Override
    public int hashCode() {
      throw new IllegalStateException("Hashing backends is dangerous!");
    }

    @Override
    public String toString() {
      return protocol + "://" + host + ':' + port;
    }
  } // end static class Backend

  /**
   * The list of backends that we can schedule on.
   * This should not generally be called directly from anywhere
   */
  public final List<Backend> backends;
  
  /** The path on the server to connect to. */
  private final String path = "";

  /** The Properties file to send to the server, serialized as JSON. */
  private final String propsAsJSON;

  /** The API key to authenticate with, or null */
  private final String apiKey;
  /** The API secret to authenticate with, or null */
  private final String apiSecret;

  /**
   * The annotation serializer responsible for translating between the wire format
   * (protocol buffers) and the {@link Annotation} classes.
   */
  private final ProtobufAnnotationSerializer serializer = new ProtobufAnnotationSerializer(true);

  /**
   * The main constructor. Create a client from a properties file and a list of backends.
   * Note that this creates at least one Daemon thread.
   *
   * @param properties The properties file, as would be passed to {@link StanfordCoreNLP}.
   * @param backends The backends to run on.
   * @param apiKey The key to authenticate with as a username
   * @param apiSecret The key to authenticate with as a password
   */
  private StanfordCoreNLPClientSimple(Properties properties, List<Backend> backends,
                                String apiKey, String apiSecret) {
    Properties serverProperties = new Properties();
    for (String key : properties.stringPropertyNames()) {
      serverProperties.setProperty(key, properties.getProperty(key));
    }
    Collections.shuffle(backends, new Random(System.currentTimeMillis()));
    this.backends = backends;
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;

    // Set required serverProperties
    serverProperties.setProperty("inputFormat", "serialized");
    serverProperties.setProperty("outputFormat", "serialized");
    serverProperties.setProperty("inputSerializer", ProtobufAnnotationSerializer.class.getName());
    serverProperties.setProperty("outputSerializer", ProtobufAnnotationSerializer.class.getName());

    // Create a list of all the properties, as JSON map elements
    List<String> jsonProperties = serverProperties.stringPropertyNames().stream().map(key -> '"' + StringUtils.escapeJsonString(key) +
            "\": \"" + StringUtils.escapeJsonString(serverProperties.getProperty(key)) + '"')
        .collect(Collectors.toList());
    // Create the JSON object
    this.propsAsJSON = "{ " + StringUtils.join(jsonProperties, ", ") + " }";
  }

  /**
   * Run on a single backend, with authentication
   *
   * @see StanfordCoreNLPClientSimple (Properties, List)
   */
  @SuppressWarnings("unused")
  public StanfordCoreNLPClientSimple(Properties properties, String host, int port,
                               String apiKey, String apiSecret) {

    this(properties, getBackends(host, port, 1), apiKey, apiSecret);
  }

  private static List<Backend> getBackends(String host, int port, int threads) {
    List<Backend> backends = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      backends.add(new Backend(host.startsWith("http://") ? "http" : "https",
              host.startsWith("http://") ? host.substring("http://".length()) : (host.startsWith("https://") ? host.substring("https://".length()) : host),
              port));
    }
    return backends;
  }

  /**
   * This method creates a sync call to the server, and blocks until the server has finished annotating the object.
   *
   * @param annotation The annotation to annotate.
   */
  public void annotate(final Annotation annotation) {
    Backend backend = this.backends.get(0);
    try {
      // 1. Create the input
      // 1.1 Create a protocol buffer
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      serializer.write(annotation, os);
      os.close();
      byte[] message = os.toByteArray();
      // 1.2 Create the query params

      String queryParams = String.format(
          "properties=%s",
          URLEncoder.encode(StanfordCoreNLPClientSimple.this.propsAsJSON, "utf-8"));

      // 2. Create a connection
      URL serverURL = new URL(backend.protocol, backend.host,
          backend.port,
          StanfordCoreNLPClientSimple.this.path + '?' + queryParams);

      // 3. Do the annotation
      //    This method has two contracts:
      //    1. It should call the two relevant callbacks
      //    2. It must not throw an exception
      doAnnotation(annotation, backend, serverURL, message, 0);
    } catch (Throwable t) {
      log.err("Could not annotate via server!", t);
      annotation.set(CoreAnnotations.ExceptionAnnotation.class, t);
    }
  }

  /**
   * Actually try to perform the annotation on the server side.
   * This is factored out so that we can retry up to 3 times.
   *
   * @param annotation The annotation we need to fill.
   * @param backend The backend we are querying against.
   * @param serverURL The URL of the server we are hitting.
   * @param message The message we are sending the server (don't need to recompute each retry).
   * @param tries The number of times we've tried already.
   */
  @SuppressWarnings("unchecked")
  private void doAnnotation(Annotation annotation, Backend backend, URL serverURL, byte[] message, int tries) {

    try {
      // 1. Set up the connection
      URLConnection connection = serverURL.openConnection();
      // 1.1 Set authentication
      if (apiKey != null && apiSecret != null) {
        String userpass = apiKey + ':' + apiSecret;
        String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
        connection.setRequestProperty("Authorization", basicAuth);
      }
      // 1.2 Set some protocol-independent properties
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/x-protobuf");
      connection.setRequestProperty("Content-Length", Integer.toString(message.length));
      connection.setRequestProperty("Accept-Charset", "utf-8");
      connection.setRequestProperty("User-Agent", StanfordCoreNLPClientSimple.class.getName());
      // 1.3 Set some protocol-dependent properties
      switch (backend.protocol) {
        case "https":
        case "http":
          ((HttpURLConnection) connection).setRequestMethod("POST");
          break;
        default:
          throw new IllegalStateException("Haven't implemented protocol: " + backend.protocol);
      }

      // 2. Annotate
      // 2.1. Fire off the request
      connection.connect();
      connection.getOutputStream().write(message);
      connection.getOutputStream().flush();
      // 2.2 Await a response
      // -- It might be possible to send more than one message, but we are not going to do that.
      Annotation response = serializer.read(connection.getInputStream()).first;
      // 2.3. Copy response over to original annotation
      for (Class key : response.keySet()) {
        annotation.set(key, response.get(key));
      }

    } catch (Throwable t) {
      // 3. We encountered an error -- retry
      if (tries < 3) {
        log.warn(t);
        doAnnotation(annotation, backend, serverURL, message, tries + 1);
      } else {
        throw new RuntimeException(t);
      }
    }
  }

  /** Return true if the referenced server is alive and returns a non-error response code.
   *
   * @param serverURL The server (running CoreNLP) to check
   * @return true if the server is alive and returns a response code between 200 and 400 inclusive
   */
  @SuppressWarnings("unused")
  public boolean checkStatus(URL serverURL) {
    try {
      // 1. Set up the connection
      HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
      // 1.1 Set authentication
      if (apiKey != null && apiSecret != null) {
        String userpass = apiKey + ':' + apiSecret;
        String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
        connection.setRequestProperty("Authorization", basicAuth);
      }

      connection.setRequestMethod("GET");
      connection.connect();
      return connection.getResponseCode() >= 200 && connection.getResponseCode() <= 400;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * Runs the entire pipeline on the content of the given text passed in.
   * @param text The text to process
   * @return An Annotation object containing the output of all annotators
   */
  public Annotation process(String text) {
    Annotation annotation = new Annotation(text);
    annotate(annotation);
    return annotation;
  }
}