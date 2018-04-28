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
package org.apache.nifi.processors.opentsdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.DateTimeUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Description: Convert object to data point
 *
 * @author bright
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"OpenTSDB", "data point", "convert"})
@CapabilityDescription("Convert object to data point!")
public class ConvertToDataPoint extends AbstractProcessor {

    private static final Relationship REL_SUCCESS = new Relationship.Builder().name("Success")
            .description("All FlowFiles that are converted to OpenTSDB Data Point are routed to this relationship").build();

    private static final Relationship REL_FAILURE = new Relationship.Builder().name("Success")
            .description("All FlowFiles that cannot be converted to OpenTSDB Data Point are routed to this relationship").build();

    private static final PropertyDescriptor FIELD_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("field-timestamp")
            .displayName("Timestamp Field")
            .description("Field as timestamp")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    private static final PropertyDescriptor FIELD_METRICS = new PropertyDescriptor.Builder()
            .name("field-metrics")
            .displayName("Metrics")
            .description("Field as metric")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    private static final PropertyDescriptor FIELD_TAGS = new PropertyDescriptor.Builder()
            .name("field-tags")
            .displayName("Tags")
            .description("Field as tags")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    private static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("convert-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of flow files to put to the database in a single transaction. Note that the contents of the "
                    + "flow files will be stored in memory until the bulk operation is performed. Also the results should be returned in the "
                    + "same order the flow files were received.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .expressionLanguageSupported(true)
            .build();

    private ComponentLog logger;

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private volatile String timestampField;

    private volatile String timestampFormatPattern;

    private volatile static ThreadLocal<DateFormat> threadLocalTimestampFormat;

    private volatile List<Rule> tags;

    private volatile List<Rule> metrics;

    private volatile int batchSize;

    @Override
    protected void init(ProcessorInitializationContext context) {
        logger = getLogger();

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELD_TIMESTAMP);
        descriptors.add(DateTimeUtils.TIMESTAMP_FORMAT);
        descriptors.add(FIELD_METRICS);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        timestampField = context.getProperty(FIELD_TIMESTAMP).evaluateAttributeExpressions().getValue();
        timestampFormatPattern = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        try {
            threadLocalTimestampFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(timestampFormatPattern));
        } catch (Exception e) {
            logger.warn("timestamp format is error: {}, set the default format 'yyyy/MM/dd HH:mm:ss'" + new Object[]{timestampFormatPattern});
            timestampFormatPattern = "yyyy/MM/dd HH:mm:ss";
            threadLocalTimestampFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(timestampFormatPattern));
        }

        tags = JSONArray.parseArray(context.getProperty(FIELD_TAGS).evaluateAttributeExpressions().getValue(), Rule.class);

        List<Rule> tempMetrics = JSONArray.parseArray(context.getProperty(FIELD_METRICS).evaluateAttributeExpressions().getValue(), Rule.class);

        if (tempMetrics.isEmpty()) {
            throw new ProcessException("Metric is empty!");
        } else {
            metrics = tempMetrics.stream().map(m -> {
                String field = m.getField();
                if (StringUtils.isBlank(field)) {
                    throw new ProcessException("Metric is error!");
                }
                return new Rule(field, StringUtils.isBlank(m.getMetric()) ? field : m.getMetric());
            }).collect(Collectors.toList());
        }

        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<DataPoint> points = toDataPoint(flowFile, session);
        if (null == points || points.isEmpty()) {
            session.transfer(flowFile, REL_FAILURE);
        } else {
            session.write(flowFile, out -> out.write(JSONArray.toJSONBytes(points)));
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private List<DataPoint> toDataPoint(final FlowFile flowFile, final ProcessSession session) {
        final StringBuilder json = new StringBuilder();
        session.read(flowFile, in -> json.append(IOUtils.toString(in, "UTF-8")));

        final String fileName = flowFile.getAttribute("filename");
        final String machineId = fileName.replace(".csv", "").replace("_5s", "").substring(fileName.indexOf("PF_F"));

        JSONObject jsonObject;
        try {
            jsonObject = JSONObject.parseObject(json.toString());
        } catch (ClassCastException e) {// the JSON array only has one element
            final JSONArray jsonArray = JSONArray.parseArray(json.toString());
            if (null == jsonArray || jsonArray.size() != 1) {
                return null;
            } else {
                jsonObject = jsonArray.getJSONObject(0);
            }
        } catch (Exception e) {
            return null;
        }

        final JSONObject obj = jsonObject;

        String timeStr = obj.getString(timestampField);
        if (StringUtils.isBlank(timeStr)) {
            logger.error("Field [{}] is blank: {}, the object detail: {}", new Object[]{timestampField, timeStr, json});
            return null;
        }

        final long timestamp;
        try {
            timestamp = threadLocalTimestampFormat.get().parse(timeStr).getTime();
        } catch (ParseException e) {
            logger.error("Failed to parse {} to timestamp by the format pattern: {}", new Object[]{timeStr, timestampFormatPattern});
            return null;
        }

        return metrics.stream().map(metric -> {
            Map<String, String> tags = new HashMap<>(1);
            tags.put("machineId", machineId);
            return new DataPoint(timestamp, metric.getMetric(), obj.getDouble(metric.getMetric()), tags);
        }).collect(Collectors.toList());
    }
}
