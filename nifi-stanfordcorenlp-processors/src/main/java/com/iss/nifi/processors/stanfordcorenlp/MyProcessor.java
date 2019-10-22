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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"Stanford", "CoreNLP"})
@CapabilityDescription("Stanford CoreNLP Processor")
@SeeAlso({})
@ReadsAttributes({
    @ReadsAttribute(attribute="text", description="One or more sentences to analyze"),
    @ReadsAttribute(attribute="annotators", description="Comma separated list of Stanford CoreNLP annotators"),
    @ReadsAttribute(attribute="format", description="Stanford CoreNLP output format, one of [json (default), xml, text]")
})
@WritesAttributes({
    @WritesAttribute(attribute="output", description="The Stanford CoreNLP analysis output rendered in the configured format")
})
public class MyProcessor extends AbstractProcessor {

    public static final String TEXT_ATTR = "text";
    public static final String ANNOTATORS_ATTR = "annotators";
    public static final String FORMAT_ATTR = "format";
    public static final String OUTPUT_ATTR = "output";
	
    public static final PropertyDescriptor TEXT_PROPERTY = new PropertyDescriptor
            .Builder().name(TEXT_ATTR)
            .displayName("Text")
            .description("One or more sentences to analyze")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ANNOTATORS_PROPERTY = new PropertyDescriptor
            .Builder().name(ANNOTATORS_ATTR)
            .displayName("Annotators")
            .description("Comma separated list of Stanford CoreNLP annotators")
            .required(false)
            .build();
    
    public static final PropertyDescriptor FORMAT_PROPERTY = new PropertyDescriptor
            .Builder().name(FORMAT_ATTR)
            .displayName("Output Format")
            .description("Stanford CoreNLP output format, one of [json (default), xml, text]")
            .required(false)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Successfully analyzed text")
            .build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Failed to analyze text")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private StanfordCoreNLPService service;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TEXT_PROPERTY);
        descriptors.add(ANNOTATORS_PROPERTY);
        descriptors.add(FORMAT_PROPERTY);
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
    public void onScheduled(final ProcessContext context) {
        ensureService();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ensureService();
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            flowFile = session.create();
        }
        String text = getAttrOrProp(TEXT_ATTR, context, flowFile);
        String annotators = getAttrOrProp(ANNOTATORS_ATTR, context, flowFile);
        String format = getAttrOrProp(FORMAT_ATTR, context, flowFile);

        // TODO: Update to reflect new output of analyze
        String output = service.analyze(text, annotators);
    }

    private static String getAttrOrProp(final String key, final ProcessContext context, final FlowFile flowFile) {
        String value = flowFile.getAttribute(key);
        if (value != null) {
            return value;
        }
        return context.getProperty(key).evaluateAttributeExpressions(flowFile)
            .getValue();
    }

    private void ensureService() {
        if (service == null) {
            service = new StanfordCoreNLPService();
        }
    }
}
