
@Slf4j
@Component
public class KafkaRecordProcessor {

    private final Tracer tracer;
    private final JsonSerializationDelegate jsonSerializationDelegate;
    private final ObjectMapper objectMapper;
    private final MetricService metricService;
    private final LifecycleAndMessageHelpers lifecycleAndMessageHelpers;

    @Autowired
    public KafkaRecordProcessor(AlfredOpenTelemetry alfredOpenTelemetry,
                                JsonSerializationDelegate jsonSerializationDelegate,
                                MetricService metricService,
                                LifecycleAndMessageHelpers lifecycleAndMessageHelpers) {
        this.tracer = alfredOpenTelemetry.getTracer();
        this.jsonSerializationDelegate = jsonSerializationDelegate;
        this.objectMapper = this.jsonSerializationDelegate.getObjectMapper();
        this.metricService = metricService;
        this.lifecycleAndMessageHelpers = lifecycleAndMessageHelpers;
    }

    private void updateHighestOffset(
            Map<TopicPartition, OffsetAndMetadata> highestOffsets,
            ConsumerRecord<UniversalSerdeData, UniversalSerdeData> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        highestOffsets.compute(partition, (key, oldValue) -> {
            long newOffset = record.offset() + 1;
            return (oldValue == null || newOffset > oldValue.offset()) ? new OffsetAndMetadata(newOffset) : oldValue;
        });
    }

    public KafkaRecordProcessingResult processRecords(
            String topic,
            Iterable<ConsumerRecord<UniversalSerdeData, UniversalSerdeData>> records,
            AbstractTargets<? extends AbstractTarget> targets,
            String friendlyName) {

        Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".processRecords").startSpan();
        long spanStartTime = Instant.now().toEpochMilli();

        String batchId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
        long batchStartTime = Instant.now().toEpochMilli();

        StringKeyCounter topicSuccessCounter = new StringKeyCounter();
        StringKeyCounter topicFailureCounter = new StringKeyCounter();

        Map<TopicPartition, OffsetAndMetadata> highestOffsets = new ConcurrentHashMap<>();
        long fetchedCount = 0L;

        try (Scope ignoredProcessRecordsScope = span.makeCurrent()) {
            List<KafkaWrapper> kafkaWrappers = new ArrayList<>();
            Map<Integer, List<MessageWrapper>> messageWrappersMap = new ConcurrentHashMap<>();

            for (ConsumerRecord<UniversalSerdeData, UniversalSerdeData> record : records) {
                fetchedCount++;
                UniversalSerdeData recordKey = record.key();
                Integer keySchemaId = Optional.ofNullable(recordKey).map(UniversalSerdeData::getSchemaId).orElse(null);
                String kafkaKey = Optional.ofNullable(recordKey).map(UniversalSerdeData::getPayload).orElse(null);

                KafkaHeaders kafkaHeaders = new KafkaHeaders();
                for (Header header : record.headers()) {
                    kafkaHeaders.add(new KafkaHeader(header.key(), header.value()));
                }

                MessageAttributes messageAttributes = new MessageAttributes(record);

                UniversalSerdeData recordValue = record.value();

                if (recordValue instanceof PoisonSerdeData poison) {
                    log.warn("[processRecords] Poison message detected on topic={} partition={} offset={}",
                            record.topic(), record.partition(), record.offset());

                    String deserializationFailureMessage =
                            ProcessingLifeCycleType.DESERIALIZATION_FAILURE.replace("{MESSAGE}", poison.getErrorMessage());
                    ProcessingLifeCycle processingLifeCycle = new ProcessingLifeCycle(friendlyName, deserializationFailureMessage);

                    MessageWrapper poisonWrapper = new MessageWrapper(
                            batchId,
                            batchStartTime,
                            StateType.EXCEPTION,
                            processingLifeCycle,
                            null,
                            null,
                            kafkaHeaders,
                            null,
                            null,
                            messageAttributes,
                            jsonSerializationDelegate
                    );

                    JsonNode diagnosticNode = objectMapper.valueToTree(poison.toDiagnostic(FieldConstants.MAX_STACK_TRACE));
                    poisonWrapper.getMessageWrapperPayload()
                            .getInternalNode()
                            .set(FieldConstants.DESERIALIZATION_DIAGNOSTIC, diagnosticNode);

                    AbstractTarget defaultTarget = targets.getDefaultTarget();
                    messageWrappersMap.computeIfAbsent(defaultTarget.getConfigId(), k -> new ArrayList<>()).add(poisonWrapper);

                    this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                            failureTags(record.topic(), FieldConstants.DESERIALIZATION), 1L);

                    this.updateHighestOffset(highestOffsets, record);

                    continue;
                }

                Integer valueSchemaId = Optional.ofNullable(recordValue).map(UniversalSerdeData::getSchemaId).orElse(null);
                String kafkaPayload = Optional.ofNullable(recordValue).map(UniversalSerdeData::getPayload).orElse(null);

                long epochMilli = Instant.now().toEpochMilli();
                kafkaWrappers.add(new KafkaWrapper(batchId, batchStartTime, epochMilli, epochMilli, messageAttributes, kafkaPayload));

                ProcessingLifeCycle ingestedMessage = new ProcessingLifeCycle(friendlyName, ProcessingLifeCycleType.INGESTED_FROM_KAFKA);
                MessageWrapper messageWrapper = new MessageWrapper(
                        batchId, batchStartTime, StateType.NEW, ingestedMessage, kafkaKey, keySchemaId, kafkaHeaders,
                        kafkaPayload, valueSchemaId, messageAttributes, jsonSerializationDelegate);

                boolean filterMatchFound = false;

                for (AbstractTarget target : targets) {
                    Filter filter = target.getFilter();

                    if (filter != null && filter.check(messageWrapper)) {
                        filterMatchFound = true;
                        MessageWrapper cloneMessageWrapper = new MessageWrapper(messageWrapper);
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(cloneMessageWrapper, friendlyName,
                                this.lifecycleAndMessageHelpers.buildTargetFoundMessage(target));
                        log.trace("[processRecords] topic={} offset={} filterConfigId={}", topic, record.offset(), filter.getConfigId());
                        processRecord(topic, target, cloneMessageWrapper, topicSuccessCounter, topicFailureCounter, friendlyName);
                        messageWrappersMap.computeIfAbsent(target.getConfigId(), k -> new ArrayList<>()).add(cloneMessageWrapper);
                    }
                }

                if (!filterMatchFound) {
                    AbstractTarget defaultTarget = targets.getDefaultTarget();
                    MessageWrapper clone = new MessageWrapper(messageWrapper);
                    this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(clone, friendlyName,
                            this.lifecycleAndMessageHelpers.buildDefaultTargetMessage(defaultTarget));
                    log.trace("[processRecords] topic={} offset={} filterConfigId={}", topic, record.offset(), defaultTarget.getConfigId());
                    processRecord(topic, defaultTarget, clone, topicSuccessCounter, topicFailureCounter, friendlyName);
                    messageWrappersMap.computeIfAbsent(defaultTarget.getConfigId(), k -> new ArrayList<>()).add(clone);
                }

                this.updateHighestOffset(highestOffsets, record);
            }

            long duration = Instant.now().toEpochMilli() - spanStartTime;
            span.setAttribute("kafkaWrappers.count", kafkaWrappers.size());
            span.setAttribute("messageWrappers.count", messageWrappersMap.values().stream().mapToInt(List::size).sum());
            this.metricService.recordHistogram(FieldConstants.KAFKA_RECORD_PROCESSOR_PROCESSING_DURATION,
                    topicTags(topic), duration);

            topicSuccessCounter.forEach((key, value) ->
                    log.debug("[processRecords] [metric] topic={} successCount={}", key, value));
            topicFailureCounter.forEach((key, value) ->
                    log.debug("[processRecords] [metric] topic={} failureCount={}", key, value));

            if (fetchedCount > 0) {
                this.metricService.incrementCounter(FieldConstants.KAFKA_TO_MONGO_SINK_FETCHED_RECORDS,
                        topicTags(topic), fetchedCount);
            }

            return new KafkaRecordProcessingResult(kafkaWrappers, messageWrappersMap, highestOffsets);
        } catch (Exception e) {
            span.recordException(e);
            span.setAttribute(FieldConstants.ERROR_OCCURRED_KEY, true);
            span.setAttribute(FieldConstants.ERROR_MESSAGE_KEY, e.getMessage());
            throw e;
        } finally {
            span.setAttribute(FieldConstants.DURATION_KEY, Instant.now().toEpochMilli() - spanStartTime);
            span.end();
        }
    }

    private void processRecord(
            String topic,
            AbstractTarget target,
            MessageWrapper messageWrapper,
            StringKeyCounter topicSuccessCounter,
            StringKeyCounter topicFailureCounter,
            String friendlyName) {

        Map<String, String> topicTags = topicTags(topic);

        MessageWrapperHeader header = messageWrapper.getMessageWrapperHeader();
        MessageWrapperPayload payload = messageWrapper.getMessageWrapperPayload();
        Integer schemaId = payload.getValueSchemaId();
        String rawPayload = payload.getKafkaPayload();

        String failureStage = FieldConstants.UNKNOWN;

        try {
            if (header.getState().equalsIgnoreCase(StateType.NEW) && target.getProcessMessageEnabled()) {
                // JSON structure validation
                if (target.getJsonStructureValidationEnabled()) {
                    try {
                        JsonNode jsonNode = this.objectMapper.readTree(rawPayload);
                        if (!jsonNode.isObject()) {
                            throw new InvalidJsonStructureException("Expected JSON Object but got " + jsonNode.getNodeType());
                        }
                        payload.setKafkaPayload(FieldConstants.NOT_APPLICABLE);
                        payload.setParsedJson(jsonNode);
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                                this.lifecycleAndMessageHelpers.buildJsonStructureValidatedMessage());
                    } catch (JsonProcessingException | InvalidJsonStructureException e) {
                        failureStage = FieldConstants.JSON_STRUCTURE_VALIDATION;
                        header.setState(StateType.EXCEPTION);
                        String msg = (e instanceof JsonProcessingException)
                                ? this.lifecycleAndMessageHelpers.buildJsonParsingErrorMessage(((JsonProcessingException) e).getOriginalMessage())
                                : this.lifecycleAndMessageHelpers.buildJsonStructureErrorMessage(e.getMessage());
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName, msg);
                        this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                                failureTags(topic, failureStage), 1L);
                        topicFailureCounter.increment(topic);
                        log.error("[processRecord] [exception] JSON structure validation failed", e);
                        return;
                    }
                }

                // JSON schema validation
                if (header.getState().equalsIgnoreCase(StateType.NEW) && target.getSchemaValidationEnabled()) {
                    try {
                         if (payload.getParsedJson() == null) {
                            throw new SchemaValidationException("Parsed JSON is null", Collections.emptySet());
                        }

                        JsonSchema jsonSchema = null;
                        try {
                            if (schemaId != null && schemaId > 0) {
                                if (!FieldConstants.NOT_APPLICABLE.equalsIgnoreCase(target.getValueSubjectOverride())) {
                                    // TODO: replace with subject override
                                    jsonSchema = target.getCachedSchemaMap().getSchemaById(schemaId);
                                } else {
                                    jsonSchema = target.getCachedSchemaMap().getSchemaById(schemaId);
                                }
                            }
                        } catch (SchemaFetchException e) {
                            failureStage = FieldConstants.RETRIEVE_FROM_SCHEMA_REGISTRY;
                            header.setState(StateType.EXCEPTION);
                            this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                                    this.lifecycleAndMessageHelpers.buildRetrieveJsonSchemaErrorMessage(e.getMessage()));

                            JsonNode diagnosticNode = objectMapper.valueToTree(e.getHttpDiagnostic(FieldConstants.MAX_STACK_TRACE));
                            payload.getInternalNode().set(FieldConstants.HTTP_DIAGNOSTIC, diagnosticNode);

                            this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                                    failureTags(topic, failureStage), 1L);
                            topicFailureCounter.increment(topic);
                            log.warn("[processRecord] [exception] Schema fetch failed for schemaId={} url={}", schemaId, e.getUrl(), e);
                            return;
                        }

                        if (jsonSchema == null) {
                            throw new SchemaValidationException("Schema is null", Collections.emptySet());
                        }

                        Set<ValidationMessage> errors = jsonSchema.validate(payload.getParsedJson());
                        if (!errors.isEmpty()) {
                            throw new SchemaValidationException(
                                    this.lifecycleAndMessageHelpers.buildSchemaValidationSummary(errors.size()), errors);
                        }

                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                                this.lifecycleAndMessageHelpers.buildJsonSchemaValidatedMessage());
                    } catch (SchemaValidationException e) {
                        failureStage = FieldConstants.RETRIEVE_FROM_SCHEMA_REGISTRY;
                        header.setState(StateType.EXCEPTION);
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                                this.lifecycleAndMessageHelpers.buildJsonSchemaErrorMessage(e.getMessage()));
                        e.getValidationMessages().forEach(vm ->
                                header.getProcessingLifeCycles().add(new ProcessingLifeCycle(friendlyName, vm.getMessage())));
                        this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                                failureTags(topic, failureStage), 1L);
                        topicFailureCounter.increment(topic);
                        log.error("[processRecord] [exception] Schema validation failed", e);
                        return;
                    }
                }

                // Transformation
                if (header.getState().equalsIgnoreCase(StateType.NEW) && target.getTransformationEnabled()) {
                    Transformation transformation = target.getTransformation();

                    if (transformation == null) {
                        failureStage = FieldConstants.TRANSFORMATION;
                        header.setState(StateType.EXCEPTION);
                        String msg = "Transformation is enabled but no transformation instance is set for targetId=" + target.getConfigId();
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName, msg);
                        this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                                failureTags(topic, failureStage), 1L);
                        topicFailureCounter.increment(topic);
                        log.warn("[processRecord] No transformation found for targetId={}", target.getConfigId());
                        return;
                    }

                    try {
                        transformation.apply(messageWrapper);
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                                this.lifecycleAndMessageHelpers.buildTransformationAppliedMessage(transformation));
                    } catch (Exception e) {
                        failureStage = FieldConstants.TRANSFORMATION;
                        header.setState(StateType.EXCEPTION);
                        this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                                this.lifecycleAndMessageHelpers.buildTransformationFailedMessage(transformation));
                        this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                                failureTags(topic, failureStage), 1L);
                        topicFailureCounter.increment(topic);
                        log.error("[processRecord] [exception] Transformation failed for targetId={}", target.getConfigId(), e);
                        return;
                    }
                }
            } else {
                this.lifecycleAndMessageHelpers.addNewProcessingLifeCycle(messageWrapper, friendlyName,
                        this.lifecycleAndMessageHelpers.buildProcessingDisabledMessage(target));
            }

            if (header.getState().equalsIgnoreCase(StateType.NEW)) {
                header.setState(StateType.COMPLETED);
                this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_SUCCESS_RECORDS_COUNT,
                        topicTags, 1L);
                topicSuccessCounter.increment(topic);
            }

        } catch (Exception e) {
            header.setState(StateType.EXCEPTION);
            this.metricService.incrementCounter(FieldConstants.KAFKA_RECORD_PROCESSOR_FAILURE_RECORDS_COUNT,
                    failureTags(topic, failureStage), 1L);
            topicFailureCounter.increment(topic);
            log.error("[processRecord] [exception] Error during record processing", e);
        }
    }

    @PreDestroy
    public void teardown() {
        log.debug("[teardown] Stopping KafkaRecordProcessor");
    }

    private Map<String, String> topicTags(String topic) {
        return Collections.singletonMap(FieldConstants.TOPIC, topic);
    }

    private Map<String, String> failureTags(String topic, String failureStage) {
        Map<String, String> tags = new HashMap<>();
        tags.put(FieldConstants.TOPIC, topic);
        tags.put(FieldNames.FAILURE_STAGE, failureStage);
        return tags;
    }
}
