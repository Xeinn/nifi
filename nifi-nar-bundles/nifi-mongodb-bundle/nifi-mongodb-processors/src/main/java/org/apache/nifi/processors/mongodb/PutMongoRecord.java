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
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@EventDriven
@Tags({"mongodb", "insert", "record", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor is a record-aware processor for inserting data into MongoDB. It uses a configured record reader and " +
        "schema to read an incoming record set from the body of a flowfile and then inserts batches of those records into " +
        "a configured MongoDB collection. This processor does not support updates, deletes or upserts. The number of documents to insert at a time is controlled " +
        "by the \"Insert Batch Size\" configuration property. This value should be set to a reasonable size to ensure " +
        "that MongoDB is not overloaded with too many inserts at once.")
public class PutMongoRecord extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();
    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("All FlowFiles that can be retried are routed to this relationship").build();

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("After sending a batch of records, MongoDB will report if individual records failed to insert. This property specifies the Controller Service to use for writing out those " +
                    "individual records sent to 'failure' or 'retry'. If this is not set, then the whole FlowFile will be routed to failure (including any records which may have been inserted " +
                    "successfully). Note that this will only be used if MongoDB reports that individual records failed, in the event that the entire FlowFile fails (e.g. in the event ES is " +
                    "down), the FF will be routed to failure without being interpreted by this record writer. If there is an error while attempting to route the failures, the entire " +
                    "FlowFile will be routed to Failure. Also if every record failed individually, the entire FlowFile will be routed to Failure without being parsed by the writer.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(false)
            .build();
    static final PropertyDescriptor INSERT_COUNT = new PropertyDescriptor.Builder()
            .name("insert_count")
            .displayName("Insert Batch Size")
            .description("The number of records to group together for one single insert operation against MongoDB.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor ORDERED_INSERTS = new PropertyDescriptor.Builder()
            .name("ordered_inserts")
            .displayName("Ordered inserts")
            .description("Instructs MongoDB to insert all records in order.  A failure will abort the batch and the failing item and all remaining items are routed to the failure relationship")
            .defaultValue("true")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor DUPLICATE_HANDLING = new PropertyDescriptor.Builder()
            .name("duplicate_handling")
            .displayName("Duplicate Handling")
            .description("Defines the action to be taken when a duplicate record is reported by MongoDB when trying to insert a record from a FlowFile.  Options are IGNORE, UPDATE or REPLACE")
            .defaultValue("IGNORE")
            .required(false)
            .allowableValues(DuplicateHandling.values())
            .build();
    static final PropertyDescriptor KEY_FIELDS = new PropertyDescriptor.Builder()
            .name("key_fields")
            .displayName("Key Fields")
            .description("Optional comma separated list of fields to be used when comparing records for the duplicate update/replace checks.  The field(s) specified must uniquely identify a " +
            "record in the collection otherwise it is possible for the update or replace action of duplicate handling to be performed against multiple records. Defaults to using the _id field.  " +
            "If no _id field on input records and duplicate handling is enabled then any duplicates will be reported as failed records with an error of DUPLICATE_KEY.")
            .defaultValue("")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("([a-zA-Z]+(?:\\.[a-zA-Z0-9_]+)*)?((\\s*\\,\\s*)[a-zA-Z]+(?:\\.[a-zA-Z0-9_]+)*)*")))
            .build();
    static final PropertyDescriptor USE_DECIMAL128 = new PropertyDescriptor.Builder()
            .name("use_decimal128")
            .displayName("Use Decimal128")
            .description("Use the Decimal128 type if available (Mongo3.4+) for Decimal values defined in the Avro Schema")
            .defaultValue("false")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(WRITE_CONCERN);
        _propertyDescriptors.add(RECORD_READER_FACTORY);
        _propertyDescriptors.add(RECORD_WRITER_FACTORY);
        _propertyDescriptors.add(INSERT_COUNT);
        _propertyDescriptors.add(ORDERED_INSERTS);
        _propertyDescriptors.add(DUPLICATE_HANDLING);
        _propertyDescriptors.add(KEY_FIELDS);
        _propertyDescriptors.add(USE_DECIMAL128);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private enum DuplicateHandling {
        IGNORE,
        UPDATE,
        REPLACE,
        FAIL
    }

    private enum InsertResultType {
        INSERTED,
        UPDATED,
        REPLACED,
        IGNORED,
        FAILED,
        DUPLICATE,
        SKIPPED
    }

    private static class InsertResult {

        InsertResultType type;
        String code;
        String message;

        public InsertResult(InsertResultType type) {
            this.type = type;
        }

        public InsertResult withErrorDetails(String code, String message) {
            this.code = code;
            this.message = message;
            return this;
        }

        public InsertResultType getType() {
            return type;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }

    public static final InsertResult INSERTED = new InsertResult(InsertResultType.INSERTED);
    public static final InsertResult UPDATED = new InsertResult(InsertResultType.UPDATED);
    public static final InsertResult REPLACED = new InsertResult(InsertResultType.REPLACED);
    public static final InsertResult IGNORED = new InsertResult(InsertResultType.IGNORED);
    public static final InsertResult FAILED = new InsertResult(InsertResultType.FAILED);
    public static final InsertResult DUPLICATE = new InsertResult(InsertResultType.DUPLICATE);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    @SuppressWarnings({"squid:S3776", "squid:S1141"})
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        final Optional<RecordSetWriterFactory> writerFactoryOptional;

        if (context.getProperty(RECORD_WRITER_FACTORY).isSet()) {
            writerFactoryOptional = Optional.of(context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class));
        } else {
            writerFactoryOptional = Optional.empty();
        }

        final WriteConcern writeConcern = getWriteConcern(context);

        final boolean orderedInserts = context.getProperty(ORDERED_INSERTS).asBoolean();
        final DuplicateHandling duplicateHandling = DuplicateHandling.valueOf(context.getProperty(DUPLICATE_HANDLING).getValue());
        final String keys = context.getProperty(KEY_FIELDS).getValue();

        List<Document> inserts = new ArrayList<>();
        List<InsertResult> results = new ArrayList<>();

        int ceiling = context.getProperty(INSERT_COUNT).asInteger();

        try (final InputStream inStream = session.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, inStream, getLogger())) {

            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);

            Record record;

            while ((record = reader.nextRecord()) != null) {

                // Convert each Record to HashMap and put into the Mongo document

                inserts.add(convertArrays(new Document(buildRecord(record, context))));

                // If inserts pending reach a specific level, trigger a write
                if (inserts.size() == ceiling) {
                    results.addAll(insertBatch(collection, inserts, orderedInserts, duplicateHandling, keys));
                    inserts = new ArrayList<>();
                }
            }

            // Flush any pending inserts
            if (!inserts.isEmpty()) {
                results.addAll(insertBatch(collection, inserts, orderedInserts, duplicateHandling, keys));
            }

        } catch (Exception ex) {
            getLogger().error("PutMongoRecord failed with error:", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

        // Were all inserts successful

        long successfulCount = results.stream().filter(e -> e.getType().equals(InsertResultType.INSERTED) || e.getType().equals(InsertResultType.UPDATED)
                || e.getType().equals(InsertResultType.REPLACED) || e.getType().equals(InsertResultType.IGNORED)).count();

        if(successfulCount == results.size()) {

            // Everything was successful, report the results
            String url = clientService != null ? clientService.getURI() : context.getProperty(URI).evaluateAttributeExpressions().getValue();
            session.getProvenanceReporter().send(flowFile, url, String.format("Added %d documents to MongoDB.", successfulCount));
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Inserted {} records into MongoDB", new Object[]{ successfulCount });
        } else {

            if(writerFactoryOptional.isPresent()) {
                splitFlowFileOutput(session, recordParserFactory, writerFactoryOptional.get(), results, flowFile);
            } else {
                getLogger().error("PutMongoRecord failed to write all records");
                session.transfer(flowFile, REL_FAILURE);
            }
        }

        session.commit();
    }

    private void splitFlowFileOutput(ProcessSession session, RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory, List<InsertResult> results, FlowFile flowFile) {

        final FlowFile inputFlowFile = flowFile;
        final FlowFile successFlowFile = session.create(inputFlowFile);
        final FlowFile failedFlowFile = session.create(inputFlowFile);
//        final FlowFile skippedFlowFile = session.create(inputFlowFile);

        long successfulCount = results.stream().filter(e -> e.getType().equals(InsertResultType.INSERTED) || e.getType().equals(InsertResultType.UPDATED)
                || e.getType().equals(InsertResultType.REPLACED) || e.getType().equals(InsertResultType.IGNORED)).count();
        long failureCount = results.stream().filter(e -> e.getType().equals(InsertResultType.FAILED)).count();

        // Set up the reader and writers
        try (final OutputStream successOut = session.write(successFlowFile);
            final OutputStream failedOut = session.write(failedFlowFile);
//            final OutputStream skippedOut = session.write(skippedFlowFile);
            final InputStream in = session.read(inputFlowFile);
            final RecordReader resultReader = readerFactory.createRecordReader(inputFlowFile, in, getLogger())) {

            final RecordSchema schema = writerFactory.getSchema(inputFlowFile.getAttributes(), resultReader.getSchema());

            try (final RecordSetWriter successWriter = writerFactory.createWriter(getLogger(), schema, successOut, successFlowFile);
                final RecordSetWriter failedWriter = writerFactory.createWriter(getLogger(), schema, failedOut, failedFlowFile)) {
//                final RecordSetWriter skippedWriter = writerFactory.createWriter(getLogger(), schema, skippedOut, skippedFlowFile)) {

                successWriter.beginRecordSet();
                failedWriter.beginRecordSet();
//                skippedWriter.beginRecordSet();

                // For each record, if it's in the failure set write it to the failure FF, otherwise it succeeded.
                Record resultRecord;
                int i = 0;
                while ((resultRecord = resultReader.nextRecord(false, false)) != null) {

                    switch(results.get(i++).getType()) {
                        case INSERTED:
                        case UPDATED:
                        case REPLACED:
                        case IGNORED:
                            successWriter.write(resultRecord);
                            break;

                        case FAILED:
                        case DUPLICATE:
                        case SKIPPED:
                            failedWriter.write(resultRecord);
                            break;
                    }
                }
            }
        } catch (Exception e) {
            // We failed while handling individual failures. Not much else we can do other than log, and route the whole thing to failure.
            getLogger().error("Failed to process {} during individual record failure handling; route whole FF to failure", new Object[] {flowFile, e});
            session.transfer(inputFlowFile, REL_FAILURE);
            if (successFlowFile != null) {
                session.remove(successFlowFile);
            }
            if (failedFlowFile != null) {
                session.remove(failedFlowFile);
            }
            // if (skippedFlowFile != null) {
            //     session.remove(skippedFlowFile);
            // }
            return;
        }

        session.putAttribute(successFlowFile, "record.count", Long.toString(successfulCount));

        // Normal behavior is to output with record.count. In order to not break backwards compatibility, set both here.
        session.putAttribute(failedFlowFile, "record.count", Long.toString(failureCount));
        session.putAttribute(failedFlowFile, "failure.count", Long.toString(failureCount));
//        session.putAttribute(skippedFlowFile, "record.count", Long.toString(results.size() - (successfulCount + failureCount)));
        session.transfer(successFlowFile, REL_SUCCESS);
        session.transfer(failedFlowFile, REL_FAILURE);
//        session.transfer(skippedFlowFile, REL_RETRY);
        session.remove(inputFlowFile);
    }

    @SuppressWarnings({"squid:S3776"})
    private List<InsertResult> insertBatch(MongoCollection<Document> collection, List<Document> inserts, boolean ordered, DuplicateHandling duplicateHandling, String keys) {

        final InsertResult inserted =  new InsertResult(InsertResultType.INSERTED);
        final InsertResult replaced = new InsertResult(InsertResultType.REPLACED);
        final InsertResult updated = new InsertResult(InsertResultType.UPDATED);
        final InsertResult ignored = new InsertResult(InsertResultType.IGNORED);
        final InsertResult skipped = new InsertResult(InsertResultType.SKIPPED);

        List<InsertResult> results = new ArrayList<>(inserts.size());

        boolean errors = false;

        while(results.size() < inserts.size() && (!errors || !ordered)) {

            int insertIndex = results.size();
            List<Document> toBeInserted = inserts.subList(insertIndex, inserts.size());

            try {

                collection.insertMany(toBeInserted, new InsertManyOptions().ordered(ordered));

                results.addAll(Collections.nCopies(toBeInserted.size(), inserted));

            } catch (MongoBulkWriteException ex) {

                if(ex.getWriteConcernError() != null) {
                    // Log the write concern error
                    getLogger().error("PutMongoRecord failed with a write concern error: " + ex.getWriteConcernError().getMessage());
                }

                if(ex.getWriteConcernError() != null && ex.getWriteErrors().isEmpty()) {

                    results.addAll(Collections.nCopies(toBeInserted.size(),
                            new InsertResult(InsertResultType.FAILED).withErrorDetails(
                                Integer.toString(ex.getWriteConcernError().getCode()),
                                ex.getWriteConcernError().getMessage())));
                } else {

                    if(ordered) {

                        // If failed in an ordered insert then all initial records will have succeeded up to the failed record and the errors
                        // list will contain a single item for the failed entry encountered

                        int failedAt = ex.getWriteErrors().get(0).getIndex();

                        if(failedAt > 0) {
                            results.addAll(Collections.nCopies(failedAt, inserted));
                        }

                        if(!duplicateHandling.equals(DuplicateHandling.FAIL)
                            && ex.getWriteErrors().get(0).getCategory().equals(ErrorCategory.DUPLICATE_KEY)) {

                            results.add(new InsertResult(InsertResultType.DUPLICATE).withErrorDetails(
                                Integer.toString(ex.getWriteErrors().get(0).getCode()),
                                ex.getWriteErrors().get(0).getMessage()));
                        } else {
                            results.add(new InsertResult(InsertResultType.FAILED).withErrorDetails(
                                Integer.toString(ex.getWriteErrors().get(0).getCode()),
                                ex.getWriteErrors().get(0).getMessage()));
                        }

                        insertIndex += failedAt;
                    } else {

                        // If failed in an unordered update then the errors list will contain a list of the entries which failed with an error

                        Map<Integer,BulkWriteError> errorList = ex.getWriteErrors().stream().collect(Collectors.toMap(BulkWriteError::getIndex, v -> v));

                        for(int i=0; i<toBeInserted.size(); ++i) {
                            if(errorList.containsKey(i)) {

                                if(!duplicateHandling.equals(DuplicateHandling.FAIL)
                                    && errorList.get(i).getCategory().equals(ErrorCategory.DUPLICATE_KEY)) {

                                    results.add(new InsertResult(InsertResultType.DUPLICATE).withErrorDetails(
                                        Integer.toString(errorList.get(i).getCode()),
                                        errorList.get(i).getMessage()));
                                } else {
                                    results.add(new InsertResult(InsertResultType.FAILED).withErrorDetails(
                                        Integer.toString(errorList.get(i).getCode()),
                                        errorList.get(i).getMessage()));
                                }
                            } else {

                                results.add(inserted);
                            }
                        }
                    }

                    // run through the results and try to convert any duplicateKey errors into successful updates or replacements

                    while(insertIndex < results.size()) {

                        InsertResult result = results.get(insertIndex);

                        if(result.getType().equals(InsertResultType.DUPLICATE)) {

                            if(duplicateHandling.equals(DuplicateHandling.IGNORE)) {
                                results.set(insertIndex, ignored);
                            } else {
                                try {
                                    if(tryUpdate(collection, inserts.get(insertIndex), keys, duplicateHandling.equals(DuplicateHandling.REPLACE))) {
                                        results.set(insertIndex, duplicateHandling.equals(DuplicateHandling.REPLACE) ? replaced : updated);
                                    } else {
                                        errors = true;
                                        break;
                                    }
                                } catch(MongoException mongoException) {

                                    results.set(insertIndex, new InsertResult(InsertResultType.FAILED).withErrorDetails(
                                        Integer.toString(mongoException.getCode()),
                                        mongoException.getMessage()));
                                    errors = true;
                                    break;
                                }
                            }
                        } else if(!result.getType().equals(InsertResultType.INSERTED)) {
                            errors = true;
                            break;
                        }

                        ++insertIndex;
                    }
                }
            }
        }

        if(results.size() < inserts.size()) {
            results.addAll(Collections.nCopies(inserts.size() - results.size(), skipped));
        }

        return results;
    }

    private boolean tryUpdate(MongoCollection<Document> collection, Document doc, String keys, boolean replace) {

        // Build a query to select the record to be updated.  If value of keys is null or empty then default to the
        // _id value - if the doc does not contain an _id value then return false.

        Map<String, Object> queryValues;

        if(keys == null || keys.isEmpty()) {

            if(!doc.containsKey("_id")) {

                return false;
            }

            queryValues = Collections.singletonMap("_id", doc.get("_id"));
        } else {

            queryValues = csvToList(keys).stream()
                .collect(Collectors.toMap(k -> k, v -> getValueFromObject(objectMapper.valueToTree(doc), dotNotationToList(v))))
                .entrySet()
                .stream()
                .filter(v -> v.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, v -> objectMapper.convertValue((ObjectNode) v, Object.class)));
        }

        if(!queryValues.isEmpty()) {

            // Perform the update/replace operation

            if(replace) {
                collection.replaceOne(new BasicDBObject(queryValues), doc);
            } else {
                collection.updateOne(new BasicDBObject(queryValues), new BasicDBObject().append("$set", doc));
            }

            return true;
        }

        return false;
    }

    private List<String> csvToList(String items) {

        return Arrays.stream(items.split("\\s*,\\s*")).map(String::trim).collect(Collectors.toList());
    }

    private List<String> dotNotationToList(String items) {

        return Arrays.stream(items.split("\\s*\\.\\s*")).map(String::trim).collect(Collectors.toList());
    }

    private JsonNode getValueFromObject(JsonNode node, List<String> fields) {

        JsonNode value = node.get(fields.get(0));

        if(value != null && fields.size() > 1) {
            return getValueFromObject(value, fields.subList(1,fields.size()));
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    private Document convertArrays(Document doc) {
        Document retVal = new Document();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getClass().isArray()) {
                retVal.put(entry.getKey(), convertArrays((Object[])entry.getValue()));
            } else if (entry.getValue() != null && (entry.getValue() instanceof Map || entry.getValue() instanceof Document)) {
                retVal.put(entry.getKey(), convertArrays(new Document((Map<String,Object>) entry.getValue())));
            } else {
                retVal.put(entry.getKey(), entry.getValue());
            }
        }

        return retVal;
    }

    @SuppressWarnings("unchecked")
    private List<Object> convertArrays(Object[] input) {
        List<Object> retVal = new ArrayList<>();
        for (Object o : input) {
            if (o != null && o.getClass().isArray()) {
                retVal.add(convertArrays((Object[])o));
            } else if (o instanceof Map) {
                retVal.add(convertArrays(new Document((Map<String,Object>)o)));
            } else {
                retVal.add(o);
            }
        }

        return retVal;
    }

    private Map<String,Object> buildRecord(final Record record, final ProcessContext context) {

        Map<String,Object> result = new HashMap<>();

        for(RecordField field : record.getSchema().getFields()) {

            result.put(field.getFieldName(), mapValue(field.getFieldName(), record.getValue(field.getFieldName()), field.getDataType(), context));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private Object mapValue(final String fieldName, final Object value, final DataType dataType, final ProcessContext context) {

        if(value == null) {

            return null;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        final Object coercedValue = DataTypeUtils.convertType(value, chosenDataType, fieldName);

        if(coercedValue == null) {

            return null;
        }

        switch (chosenDataType.getFieldType()) {

            case DATE:
            case TIME:
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case BYTE:
            case SHORT:
            case CHAR:
            case STRING:
            case BIGINT:
            case BOOLEAN:
                return coercedValue;

            case TIMESTAMP:
                return ((Timestamp) coercedValue).toInstant();

            // case DECIMAL:
            //     return context.getProperty(USE_DECIMAL128).asBoolean().booleanValue()
            //             ? buildDecimal128orDouble((BigDecimal) coercedValue) : ((BigDecimal) coercedValue).doubleValue();

            case RECORD:
                return buildRecord((Record) coercedValue, context);

            case ARRAY:
                // Map the value of each element of the array
                return Arrays.stream((Object[]) coercedValue)
                    .map(v -> mapValue(fieldName, v, ((ArrayDataType) chosenDataType).getElementType(), context)).collect(Collectors.toList()).toArray();

            case MAP: {
                // Map the values of each entry in the map

                Map<String,Object> result = new HashMap<>();

                for(Map.Entry<String,Object> entry : ((Map<String, Object>) coercedValue).entrySet()) {

                    result.put(entry.getKey(), mapValue(entry.getKey(), entry.getValue(), ((MapDataType) chosenDataType).getValueType(), context));
                }

                return result;
            }

            default:
                return coercedValue.toString();
        }
    }

    /***
     * Return a Double or Decimal128 value.
     *
     * If the Decimal128 type is available (mongodb driver >= 3.4) then this method will return a Decimal128 for the BigDecimal
     * otherwise it will revert to using a Double value
     *
     * @param value BigDecimal value to be converted to Decimal128 or Double
     * @return Returns a Decimal128 type object or a Double type object
     */
    private Object buildDecimal128orDouble(BigDecimal value) {

        try {
            Class<?> clazz = Class.forName("org.bson.types.Decimal128");

            return clazz.getConstructor(BigDecimal.class).newInstance(value);

        } catch(LinkageError | ClassNotFoundException ex) {

            getLogger().info("org.bson.types.Decimal128 type not available, using double value instead");

            return value.doubleValue();
        } catch(NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException | IllegalArgumentException | SecurityException ex) {

            getLogger().info("Failed to construct a new instance of org.bson.types.Decimal128, using double value instead");

            return value.doubleValue();
        }
    }
}