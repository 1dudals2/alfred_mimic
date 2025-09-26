@Slf4j
@Component
public class MongoSaver {

    private final Tracer tracer;
    private final MetricService metricService;

    private final InsertManyOptions insertManyOptions;

    private final KafkaGlobalCollectionRegistry kafkaGlobalCollectionRegistry;
    private final KafkaGlobalCollections kafkaGlobalCollections;
    private final MongoTargetRegistry mongoTargetRegistry;
    private final MongoOutboxSourceRegistry mongoOutboxSourceRegistry;
    private final JsonSerializationDelegate jsonSerializationDelegate;
    private final ObjectMapper objectMapper;
    private final FindOneAndUpdateOptions findOneAndUpdateOptions;

    @Autowired
    public MongoSaver(
            AlfredOpenTelemetry alfredOpenTelemetry,
            MetricService metricService,
            KafkaGlobalCollectionRegistry kafkaGlobalCollectionRegistry,
            MongoTargetRegistry mongoTargetRegistry,
            JsonSerializationDelegate jsonSerializationDelegate,
            MongoOutboxSourceRegistry mongoOutboxSourceRegistry) {
        this.tracer = alfredOpenTelemetry.getTracer();
        this.metricService = metricService;
        this.kafkaGlobalCollectionRegistry = kafkaGlobalCollectionRegistry;
        this.kafkaGlobalCollections = this.kafkaGlobalCollectionRegistry.getKafkaGlobalCollections();
        this.mongoTargetRegistry = mongoTargetRegistry;
        this.jsonSerializationDelegate = jsonSerializationDelegate;
        this.objectMapper = this.jsonSerializationDelegate.getObjectMapper();
        this.mongoOutboxSourceRegistry = mongoOutboxSourceRegistry;

        this.insertManyOptions = new InsertManyOptions().ordered(false);
        this.findOneAndUpdateOptions = new FindOneAndUpdateOptions()
                .upsert(true)
                .returnDocument(com.mongodb.client.model.ReturnDocument.AFTER);
    }

    public void saveKafkaWrappers(List<KafkaWrapper> kafkaWrappers) {

        if (!kafkaWrappers.isEmpty()) {
            Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".saveKafkaWrappers").startSpan();
            long startTime = System.currentTimeMillis();

            Attributes attributes = Attributes.builder()
                    .put(FieldConstants.DATABASE_KEY, this.kafkaGlobalCollections.getDatabaseName())
                    .put(FieldConstants.COLLECTION_KEY, this.kafkaGlobalCollections.getKafkaPayloadCollection())
                    .build();
            Map<String, String> metricTags = buildCollectionTags(
                    this.kafkaGlobalCollections.getDatabaseName(),
                    this.kafkaGlobalCollections.getKafkaPayloadCollection());

            try (Scope ignoredSaveKafkaWrappersScope = span.makeCurrent()) {

                span.setAllAttributes(attributes);

                try {
                    // write to collection
                    MongoCollection<KafkaWrapper> kafkaWrapperCollection =
                            this.kafkaGlobalCollectionRegistry.getKafkaPayloadCollection();

                    InsertManyResult insertManyResult = kafkaWrapperCollection.insertMany(kafkaWrappers, insertManyOptions);

                    int insertedCount = insertManyResult.getInsertedIds().size();
                    long duration = System.currentTimeMillis() - startTime;

                    log.info("[saveKafkaWrappers] [metric] kafkaWrappersCount={} insertedCount={} duration={}",
                            kafkaWrappers.size(), insertedCount, duration);

                    span.setAttribute("mongo.insertedCount", insertedCount);
                    this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_SUCCESS_RECORDS_COUNT,
                            metricTags, (long) insertedCount);
                } catch (MongoBulkWriteException mongoBulkWriteException) {
                    long duration = System.currentTimeMillis() - startTime;

                    int totalCount = kafkaWrappers.size();
                    int failureCount = mongoBulkWriteException.getWriteErrors().size();
                    int insertedCount = totalCount - failureCount;

                    log.info("[saveKafkaWrappers] [metric] kafkaWrappersCount={} insertedCount={} failureCount={} duration={}",
                            kafkaWrappers.size(), insertedCount, failureCount, duration);

                    span.setAttribute("mongo.insertedCount", insertedCount);
                    this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_SUCCESS_RECORDS_COUNT,
                            metricTags, (long) insertedCount);

                    saveKafkaWrappersCatchAll(kafkaWrappers, mongoBulkWriteException);
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("error.message", e.getMessage());
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                this.metricService.recordHistogram(FieldConstants.MONGODB_SAVER_SUCCESS_SAVING_DURATION,
                        metricTags, duration);
                span.setAttribute("saving.duration.ms", duration);
                span.end();
            }
        }
    }

    public void saveKafkaWrappersCatchAll(
            List<KafkaWrapper> kafkaWrappers,
            MongoBulkWriteException mongoBulkWriteException) {

        if (!kafkaWrappers.isEmpty()) {
            Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".saveKafkaWrappersCatchAll").startSpan();
            long startTime = System.currentTimeMillis();

            Attributes attributes = Attributes.builder()
                    .put(FieldConstants.DATABASE_KEY, this.kafkaGlobalCollections.getDatabaseName())
                    .put(FieldConstants.COLLECTION_KEY, this.kafkaGlobalCollections.getKafkaCatchAllCollection())
                    .build();
            Map<String, String> metricTags = buildCollectionTags(
                    this.kafkaGlobalCollections.getDatabaseName(),
                    this.kafkaGlobalCollections.getKafkaCatchAllCollection());

            try (Scope ignoredSaveKafkaWrappersCatchAllScope = span.makeCurrent()) {

                span.setAllAttributes(attributes);

                // build errorMessageWrappers
                List<BulkWriteError> writeErrors = mongoBulkWriteException.getWriteErrors();
                List<CatchAllWrapper> errorMessageWrappers = new ArrayList<>();

                writeErrors.forEach(writeError -> {
                    KafkaWrapper kafkaWrapper = kafkaWrappers.get(writeError.getIndex());
                    String errorMessage = writeError.getMessage();
                    CatchAllWrapper catchAllWrapper = new CatchAllWrapper(
                            WrapperType.KAFKA_WRAPPER, errorMessage, this.jsonSerializationDelegate.toJson(kafkaWrapper));
                    errorMessageWrappers.add(catchAllWrapper);
                });

                // write to collection
                MongoCollection<CatchAllWrapper> kafkaCatchAllCollection =
                        this.kafkaGlobalCollectionRegistry.getKafkaCatchAllCollection();

                InsertManyResult insertManyResult =
                        kafkaCatchAllCollection.insertMany(errorMessageWrappers, insertManyOptions);

                int insertedCount = insertManyResult.getInsertedIds().size();
                long duration = System.currentTimeMillis() - startTime;

                log.info("[saveKafkaWrappersCatchAll] [metric] errorMessageWrappersCount={} insertedCount={} duration={}",
                        errorMessageWrappers.size(), insertedCount, duration);

                span.setAttribute("mongo.insertedCount", insertedCount);
                this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_RECORDS_COUNT,
                        metricTags, (long) insertedCount);
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("error.message", e.getMessage());
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                this.metricService.recordHistogram(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_DURATION,
                        metricTags, duration);
                span.setAttribute("saving.duration.ms", duration);
                span.end();
            }
        }
    }

    public void saveMessageWrappers(Map<Integer, List<MessageWrapper>> messageWrappersMap, String friendlyName) {

        if (!messageWrappersMap.isEmpty()) {
            Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".saveMessageWrappers").startSpan();
            long startTime = System.currentTimeMillis();

            try (Scope ignoredSaveMessageWrappersScope = span.makeCurrent()) {
                for (Map.Entry<Integer, List<MessageWrapper>> messageWrappersMapEntry : messageWrappersMap.entrySet()) {

                    Integer mongoTargetConfigId = messageWrappersMapEntry.getKey();

                    MongoTarget mongoTarget = this.mongoTargetRegistry.getMongoTarget(mongoTargetConfigId);
                    if (mongoTarget == null) {
                        log.error("[saveMessageWrappers] mongoTarget is null : {}", mongoTargetConfigId);
                        throw new IllegalArgumentException("mongoTarget is null" + mongoTargetConfigId);
                    }

                    List<MessageWrapper> messageWrappers = messageWrappersMapEntry.getValue();
                    saveMessageWrapper(mongoTarget, messageWrappers, friendlyName);
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("error.message", e.getMessage());
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                span.setAttribute("saving.duration.ms", duration);
                span.end();
            }
        }
    }

    public void saveMessageWrapper(MongoTarget mongoTarget, List<MessageWrapper> messageWrappers, String friendlyName) {

        if (!messageWrappers.isEmpty()) {
            Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".saveMessageWrapper").startSpan();
            long startTime = System.currentTimeMillis();

            Attributes attributes = Attributes.builder()
                    .put(FieldConstants.DATABASE_KEY, mongoTarget.getDatabaseName())
                    .put(FieldConstants.COLLECTION_KEY, mongoTarget.getTargetCollection())
                    .build();
            Map<String, String> metricTags = buildCollectionTags(mongoTarget.getDatabaseName(),
                    mongoTarget.getTargetCollection());

            try (Scope ignoredSaveMessageWrapperScope = span.makeCurrent()) {

                span.setAllAttributes(attributes);

                try {
                    MongoDatabase mongoDatabase = mongoTarget.getMongoClient().getDatabase(mongoTarget.getDatabaseName());

                    // write to collection
                    MongoCollection<MessageWrapper> targetCollection =
                            mongoDatabase.getCollection(mongoTarget.getTargetCollection(), MessageWrapper.class);

                    InsertManyResult insertManyResult = targetCollection.insertMany(messageWrappers, insertManyOptions);

                    int insertedCount = insertManyResult.getInsertedIds().size();
                    long duration = System.currentTimeMillis() - startTime;

                    log.info("[saveMessageWrapper] [metric] mongoTargetConfigId={} messageWrappersCount={} insertedCount={} duration={}",
                            mongoTarget.getConfigId(), messageWrappers.size(), insertedCount, duration);

                    span.setAttribute("mongo.insertedCount", insertedCount);
                    this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_SUCCESS_RECORDS_COUNT,
                            metricTags, (long) insertedCount);
                } catch (MongoBulkWriteException mongoBulkWriteException) {
                    long duration = System.currentTimeMillis() - startTime;

                    int totalCount = messageWrappers.size();
                    int failureCount = mongoBulkWriteException.getWriteErrors().size();
                    int insertedCount = totalCount - failureCount;

                    log.info("[saveMessageWrapper] [metric] mongoTargetConfigId={} messageWrappersCount={} insertedCount={} failureCount={} duration={}",
                            mongoTarget.getConfigId(), messageWrappers.size(), insertedCount, failureCount, duration);

                    span.setAttribute("mongo.insertedCount", insertedCount);
                    this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_SUCCESS_RECORDS_COUNT,
                            metricTags, (long) insertedCount);

                    handleSaveMessageWrapperException(messageWrappers, mongoBulkWriteException, friendlyName);
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("error.message", e.getMessage());
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                this.metricService.recordHistogram(FieldConstants.MONGODB_SAVER_SUCCESS_SAVING_DURATION,
                        metricTags, duration);
                span.setAttribute("saving.duration.ms", duration);
                span.end();
            }
        }
    }

    public void handleSaveMessageWrapperException(
            List<MessageWrapper> messageWrappers, MongoBulkWriteException mongoBulkWriteException, String friendlyName) {

        if (!messageWrappers.isEmpty()) {
            Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".handleSaveMessageWrapperException").startSpan();
            long startTime = System.currentTimeMillis();

            Attributes attributes = Attributes.builder()
                    .put(FieldConstants.DATABASE_KEY, this.kafkaGlobalCollections.getDatabaseName())
                    .put(FieldConstants.COLLECTION_KEY, this.kafkaGlobalCollections.getKafkaCatchAllCollection())
                    .build();
            Map<String, String> metricTags = buildCollectionTags(
                    this.kafkaGlobalCollections.getDatabaseName(),
                    this.kafkaGlobalCollections.getKafkaCatchAllCollection());

            try (Scope ignoredHandleSaveMessageWrapperExceptionScope = span.makeCurrent()) {

                span.setAllAttributes(attributes);

                // build errorMessageWrappers
                List<BulkWriteError> writeErrors = mongoBulkWriteException.getWriteErrors();
                List<MessageWrapper> errorMessageWrappers = new ArrayList<>();

                writeErrors.forEach(writeError -> {
                    MessageWrapper messageWrapper = messageWrappers.get(writeError.getIndex());
                    String message = "Error saving to collection :: " + writeError.getMessage();
                    ProcessingLifeCycle insertErrorMessage = new ProcessingLifeCycle(friendlyName, message);

                    messageWrapper.getMessageWrapperHeader().setState(StateType.EXCEPTION);
                    messageWrapper.getMessageWrapperHeader().getProcessingLifeCycles().add(insertErrorMessage);

                    errorMessageWrappers.add(messageWrapper);
                });

                // write to collection
                try {
                    MongoCollection<MessageWrapper> kafkaRejectCollection =
                            this.kafkaGlobalCollectionRegistry.getKafkaRejectCollection();

                    InsertManyResult insertManyResult =
                            kafkaRejectCollection.insertMany(errorMessageWrappers, insertManyOptions);

                    int insertedCount = insertManyResult.getInsertedIds().size();
                    long duration = System.currentTimeMillis() - startTime;

                    log.info("[handleSaveMessageWrapperException] [metric] errorMessageWrappersCount={} insertedCount={} duration={}",
                            errorMessageWrappers.size(), insertedCount, duration);

                    span.setAttribute("mongo.insertedCount", insertedCount);
                    this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_RECORDS_COUNT,
                            metricTags, (long) insertedCount);
                } catch (MongoBulkWriteException bulkWriteException) {
                    long duration = System.currentTimeMillis() - startTime;

                    int totalCount = messageWrappers.size();
                    int failureCount = bulkWriteException.getWriteErrors().size();
                    int insertedCount = totalCount - failureCount;

                    log.info("[handleSaveMessageWrapperException] [metric] errorMessageWrappersCount={} insertedCount={} failureCount={} duration={}",
                            messageWrappers.size(), insertedCount, failureCount, duration);

                    span.setAttribute("mongo.insertedCount", insertedCount);
                    this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_RECORDS_COUNT,
                            metricTags, (long) insertedCount);

                    saveMessageWrapperCatchAll(errorMessageWrappers, bulkWriteException);
                }
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("error.message", e.getMessage());
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                this.metricService.recordHistogram(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_DURATION,
                        metricTags, duration);
                span.setAttribute("saving.duration.ms", duration);
                span.end();
            }
        }
    }

    private void saveMessageWrapperCatchAll(
            List<MessageWrapper> messageWrappers, MongoBulkWriteException mongoBulkWriteException) {

        if (!messageWrappers.isEmpty()) {
            Span span = this.tracer.spanBuilder(this.getClass().getSimpleName() + ".saveMessageWrapperCatchAll").startSpan();
            long startTime = System.currentTimeMillis();

            Attributes attributes = Attributes.builder()
                    .put(FieldConstants.DATABASE_KEY, this.kafkaGlobalCollections.getDatabaseName())
                    .put(FieldConstants.COLLECTION_KEY, this.kafkaGlobalCollections.getKafkaCatchAllCollection())
                    .build();
            Map<String, String> metricTags = buildCollectionTags(
                    this.kafkaGlobalCollections.getDatabaseName(),
                    this.kafkaGlobalCollections.getKafkaCatchAllCollection());

            try (Scope ignoredSaveMessageWrapperCatchAllScope = span.makeCurrent()) {

                span.setAllAttributes(attributes);

                // build errorMessageWrappers
                List<BulkWriteError> writeErrors = mongoBulkWriteException.getWriteErrors();
                List<CatchAllWrapper> errorMessageWrappers = new ArrayList<>();

                writeErrors.forEach(writeError -> {
                    MessageWrapper messageWrapper = messageWrappers.get(writeError.getIndex());
                    String errorMessage = writeError.getMessage();
                    CatchAllWrapper catchAllWrapper = new CatchAllWrapper(
                            WrapperType.MESSAGE_WRAPPER, errorMessage, this.jsonSerializationDelegate.toJson(messageWrapper));
                    errorMessageWrappers.add(catchAllWrapper);
                });

                // write to collection
                MongoCollection<CatchAllWrapper> kafkaCatchAllCollection =
                        this.kafkaGlobalCollectionRegistry.getKafkaCatchAllCollection();

                InsertManyResult insertManyResult =
                        kafkaCatchAllCollection.insertMany(errorMessageWrappers, insertManyOptions);

                int insertedCount = insertManyResult.getInsertedIds().size();
                long duration = System.currentTimeMillis() - startTime;

                log.info("[saveMessageWrapperCatchAll] [metric] errorMessageWrappersCount={} insertedCount={} duration={}",
                        errorMessageWrappers.size(), insertedCount, duration);

                span.setAttribute("mongo.insertedCount", insertedCount);
                this.metricService.incrementCounter(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_RECORDS_COUNT,
                        metricTags, (long) insertedCount);
            } catch (Exception e) {
                span.recordException(e);
                span.setAttribute("error", true);
                span.setAttribute("error.message", e.getMessage());
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                this.metricService.recordHistogram(FieldConstants.MONGODB_SAVER_FAILURE_HANDLING_DURATION,
                        metricTags, duration);
                span.setAttribute("saving.duration.ms", duration);
                span.end();
            }
        }
    }

    @PreDestroy
    public void teardown() {
        log.debug("[teardown] Stopping MongoSaver");
    }

    private Map<String, String> buildCollectionTags(String database, String collection) {
        Map<String, String> tags = new HashMap<>();
        tags.put(FieldConstants.DATABASE, database);
        tags.put(FieldConstants.COLLECTION, collection);
        return tags;
    }
}
