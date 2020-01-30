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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import edu.stanford.nlp.pipeline.AnnotationPipeline;

@Tags({ "Stanford", "CoreNLP" })
@CapabilityDescription("Stanford CoreNLP Processor")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = ""), })
@WritesAttributes({
        @WritesAttribute(attribute = "output", description = "The Stanford CoreNLP analysis output rendered in the configured format") })
public class StanfordCoreNLPProcessor extends AbstractProcessor {
    public static final String ENTITIES_ATTR = "entityTypes";
    public static final String PATH_ATTR = "path";
    public static final String PROPS_ATTR = "jsonProps";
    public static final String HOST_ATTR = "host";
    public static final String PORT_ATTR = "port";
    public static final String KEY_ATTR = "apiKey";
    public static final String SECRET_ATTR = "apiSecret";
    public static final String OUTPUT_ATTR = "output";

    public static final PropertyDescriptor ENTITIES_PROPERTY = new PropertyDescriptor.Builder().name(ENTITIES_ATTR)
            .displayName("Entity Types")
            .description(
                    "Lowercase comma separated list of NER tags to extract from text, such as: location,organization")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor PATH_PROPERTY = new PropertyDescriptor.Builder().name(PATH_ATTR)
            .displayName("JSON Path")
            .description(
                    "The JSON Path (https://github.com/json-path) from incoming flow file to extract for analyzing, such as: $.['title','content'] (if not specified, flow file will be treated as plain text)")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor PROPS_PROPERTY = new PropertyDescriptor.Builder().name(PROPS_ATTR)
            .displayName("StanfordCoreNLP Props as JSON")
            .description(
                    "Properties to configure the StanfordCoreNLP object or StanfordCoreNLPClient object as JSON, such as: {\"annotators\": \"tokenize,ssplit,pos,lemma,ner\", \"threads\": 1}")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor HOST_PROPERTY = new PropertyDescriptor.Builder().name(HOST_ATTR)
            .displayName("StanfordCoreNLPClient Host")
            .description(
                    "StanfordCoreNLPClient host address, such as: http://localhost (if not specified, local processing will be performed)")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor PORT_PROPERTY = new PropertyDescriptor.Builder().name(PORT_ATTR)
            .displayName("StanfordCoreNLPClient Port").description("StanfordCoreNLPClient port, such as: 9000")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor KEY_PROPERTY = new PropertyDescriptor.Builder().name(KEY_ATTR)
            .displayName("StanfordCoreNLPClient API Key")
            .description("StanfordCoreNLPClient API Key for servers that have authentication configured, not required")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor SECRET_PROPERTY = new PropertyDescriptor.Builder().name(SECRET_ATTR)
            .displayName("StanfordCoreNLPClient API Secret")
            .description(
                    "StanfordCoreNLPClient API Secret for servers that have authentication configured, not required")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("success")
            .description("Successfully analyzed text").build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder().name("failure")
            .description("Failed to analyze text").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private StanfordCoreNLPService service;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ENTITIES_PROPERTY);
        descriptors.add(PATH_PROPERTY);
        descriptors.add(PROPS_PROPERTY);
        descriptors.add(HOST_PROPERTY);
        descriptors.add(PORT_PROPERTY);
        descriptors.add(KEY_PROPERTY);
        descriptors.add(SECRET_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {
        getLogger().debug("OnScheduled called for StanfordCoreNLPProcessor, refreshing StanfordCoreNLPService");
        service = new StanfordCoreNLPService(createPipeline(context));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ensureService(context);

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        final String flowFileText = getTextFromSession(session, flowFile);

        if (flowFileText == null || flowFileText.isEmpty()) {
            getLogger().error("Empty flow file cannot be analyzed");
            session.transfer(flowFile, FAILURE_RELATIONSHIP);
            return;
        }

        final String jsonPath = context.getProperty(PATH_ATTR).evaluateAttributeExpressions(flowFile).getValue();
        final String entityTypes = context.getProperty(ENTITIES_ATTR).evaluateAttributeExpressions(flowFile).getValue();
        final String text = getTextFromJson(flowFileText, jsonPath);
        Map<String, List<String>> entityMap;

        try {
            entityMap = service.extractEntities(text, entityTypes);
        } catch (final Exception e) {
            e.printStackTrace();
            getLogger().error("Failed to analyze flow file text");
            session.transfer(flowFile, FAILURE_RELATIONSHIP);
            return;
        }

        Map<String, Object> flowFileJsonMap;

        final Gson gson = new Gson();
        try {
            flowFileJsonMap = gson.fromJson(flowFileText, Map.class);
        } catch (final JsonSyntaxException e) {
            e.printStackTrace();
            getLogger().warn("Failed to parse flow file text as json, writing new flow file from blank json document");
            flowFileJsonMap = new HashMap<String, Object>();
        }

        try {
            for (final String k : entityMap.keySet()) {
                flowFileJsonMap.put(k, entityMap.get(k));
            }

            final String entityJson = gson.toJson(entityMap);
            final String finalJson = gson.toJson(flowFileJsonMap);

            flowFile = session.putAttribute(flowFile, OUTPUT_ATTR, entityJson);
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(finalJson.getBytes());
                }
            });

            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
            return;
        } catch (final Exception e) {
            e.printStackTrace();
            getLogger().warn("Failed to generate flow file or attributes");
        }

        session.transfer(flowFile, FAILURE_RELATIONSHIP);
    }

    private String getTextFromSession(final ProcessSession session, final FlowFile flowFile) {
        final AtomicReference<String> atomicText = new AtomicReference<>();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                try {
                    final String rawText = IOUtils.toString(in);
                    atomicText.set(rawText);
                } catch (final NullPointerException e) {
                    e.printStackTrace();
                    getLogger().warn("FlowFile text was null");
                } catch (final IOException e) {
                    e.printStackTrace();
                    getLogger().error("FlowFile text could not be read due to IOException");
                }
            }
        });

        final String text = atomicText.get();
        if (text == null || text.isEmpty()) {
            return null;
        }

        return text;
    }

    private String getTextFromJson(final String flowFileText, final String jsonPath) {
        if (jsonPath == null || jsonPath.isEmpty()) {
            return flowFileText;
        }

        try {
            final Configuration conf = Configuration.builder().options(Option.ALWAYS_RETURN_LIST).build();
            final List<String> result = JsonPath.using(conf).parse(flowFileText).read(jsonPath);
            final String combined = String.join(" ", result);
            return combined;
        } catch (final ClassCastException e) {
            final LinkedHashMap<String, Object> resultMap = JsonPath.read(flowFileText, jsonPath);
            String combined = "";
            for (final String k : resultMap.keySet()) {
                combined += " " + resultMap.get(k);
            }
            return combined;
        } catch (final Exception e) {
            e.printStackTrace();
            getLogger().warn("Failed to parse json using specified json path, analyzing flow file as text");
        }

        return flowFileText;
    }

    private int getPort(final ProcessContext context) {
        int port;
        try {
            port = context.getProperty(PORT_ATTR).asInteger();
        } catch (final NumberFormatException e) {
            e.printStackTrace();
            getLogger().error("Failed to read port as integer, using default 9000");
            port = 9000;
        }
        return port;
    }

    private AnnotationPipeline createPipeline(final ProcessContext context) {
        final String jsonProps = context.getProperty(PROPS_ATTR).getValue();
        final Properties props = jsonToProps(jsonProps);
        final String host = context.getProperty(HOST_ATTR).getValue();

        if (host == null) {
            return StanfordCoreNLPService.createPipeline(props);
        }

        final int port = getPort(context);
        final String key = context.getProperty(KEY_ATTR).getValue();
        final String secret = context.getProperty(SECRET_ATTR).getValue();

        return StanfordCoreNLPService.createPipeline(props, host, port, key, secret);
    }

    private void ensureService(final ProcessContext context) {
        if (service != null) {
            return;
        }

        service = new StanfordCoreNLPService(createPipeline(context));
        return;
    }

    private Properties jsonToProps(final String jsonProps) {
        final Properties props = new Properties();
        if (jsonProps == null) {
            return props;
        }
        final Gson gson = new Gson();
        try {
            final Map<String, Object> jsonMap = gson.fromJson(jsonProps, Map.class);
            for (final String k : jsonMap.keySet()) {
                props.setProperty(k, jsonMap.get(k).toString());
            }
        } catch (final JsonSyntaxException e) {
            e.printStackTrace();
            getLogger().error("Failed to read json string.");
        }
        return props;
    }
}
