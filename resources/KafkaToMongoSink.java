package com.alfred.kafka.sink;

import com.alfred.metrics.FieldConstants;
import com.alfred.metrics.MetricService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Kafka to Mongo sink task that processes Kafka records and persists them into Mongo targets.
 * Emits metrics via {@link MetricService} to provide observability into throughput and latency.
 */
@Slf4j
@Component
public class KafkaToMongoSink extends AbstractTask {

    private final String friendlyName = this.getClass().getSimpleName();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final Environment environment;
    private final AppConfiguration appConfiguration;
    private final KafkaConsumerRegistry kafkaConsumerRegistry;

    private final MongoTargetRegistry mongoTargetRegistry;
    private final KafkaRecordProcessor kafkaRecordProcessor;
    private final MongoSaver mongoSaver;

    private final AlfredOpenTelemetry alfredOpenTelemetry;
    private final Tracer tracer;
    private final JsonSerializationDelegate jsonSerializationDelegate;
    private final ObjectMapper objectMapper;
    private final GeneralHelpers generalHelpers;
    private final MetricService metricService;

    private KafkaToMongoSinkTargets kafkaToMongoSinkTargets;

    @Autowired
    public KafkaToMongoSink(
            Environment environment,
            AppConfiguration appConfiguration,
            KafkaConsumerRegistry kafkaConsumerRegistry,
            MongoTargetRegistry mongoTargetRegistry,
            KafkaRecordProcessor kafkaRecordProcessor,
            MongoSaver mongoSaver,
            AlfredOpenTelemetry alfredOpenTelemetry,
            JsonSerializationDelegate jsonSerializationDelegate,
            GeneralHelpers generalHelpers,
            MetricService metricService) {
        this.environment = environment;
        this.appConfiguration = appConfiguration;
        this.kafkaConsumerRegistry = kafkaConsumerRegistry;
        this.mongoTargetRegistry = mongoTargetRegistry;
        this.kafkaRecordProcessor = kafkaRecordProcessor;
        this.mongoSaver = mongoSaver;
        this.alfredOpenTelemetry = alfredOpenTelemetry;
        this.tracer = this.alfredOpenTelemetry.getTracer();
        this.jsonSerializationDelegate = jsonSerializationDelegate;
        this.objectMapper = this.jsonSerializationDelegate.getObjectMapper();
        this.generalHelpers = generalHelpers;
        this.metricService = metricService;
    }

    @PostConstruct
    @Override
    public void init() {
        log.debug("[init] Start initializing {}", friendlyName);

        this.kafkaToMongoSinkTargets = new KafkaToMongoSinkTargets();

        ConfigurationWrappers kafkaToMongoSinkConfigWrappers =
                this.appConfiguration.getComponentConfiguration(ConfigNames.KAFKA_TO_MONGO_SINK_CONFIG);

        if (kafkaToMongoSinkConfigWrappers == null) {
            log.debug("[init] kafkaToMongoSinkConfigWrappers is null");
        } else {
            log.debug("[init] kafkaToMongoSinkConfigWrappers : {}",
                    this.jsonSerializationDelegate.toJson(kafkaToMongoSinkConfigWrappers));

            processKafkaToMongoSinkConfigWrappers(kafkaToMongoSinkConfigWrappers);
        }

        log.debug("[init] Finish initializing {}", friendlyName);
    }

    private void processKafkaToMongoSinkConfigWrappers(ConfigurationWrappers kafkaToMongoSinkConfigWrappers) {
        for (ConfigurationWrapper kafkaToMongoSinkConfigWrapper : kafkaToMongoSinkConfigWrappers) {
            try {
                String payload = kafkaToMongoSinkConfigWrapper.getPayload();
                KafkaToMongoSinkConfig kafkaToMongoSinkConfig =
                        this.objectMapper.readValue(payload, KafkaToMongoSinkConfig.class);

                if (kafkaToMongoSinkConfig.getEnabled()) {

                    Integer kafkaConsumerConfigId = kafkaToMongoSinkConfig.getKafkaConsumerConfigId();

                    if (!this.kafkaConsumerRegistry.containsKey(kafkaConsumerConfigId)) {
                        log.error("[init] Kafka consumer config not found : {}", kafkaConsumerConfigId);
                        throw new InitException("Kafka consumer config not found : " + kafkaConsumerConfigId);
                    }

                    KafkaToMongoSinkTarget kafkaToMongoSinkTarget = this.kafkaToMongoSinkTargets.get(kafkaConsumerConfigId);

                    if (kafkaToMongoSinkTarget == null) {
                        KafkaConsumerStruct kafkaConsumerStruct =
                                this.kafkaConsumerRegistry.getKafkaConsumerStruct(kafkaConsumerConfigId);

                        KafkaConsumer<UniversalSerdeData, UniversalSerdeData> kafkaConsumer
                                = kafkaConsumerStruct.getKafkaConsumer();
                        if (kafkaConsumer == null) {
                            log.error("[init] Kafka consumer is null : {}", kafkaConsumerConfigId);
                            throw new InitException("Kafka consumer is null : " + kafkaConsumerConfigId);
                        }

                        Duration pollingTimeout = kafkaConsumerStruct.getKafkaPollingTimeout();
                        if (pollingTimeout == null) {
                            log.error("[init] Polling timeout is null : {}", kafkaConsumerConfigId);
                            throw new InitException("Polling timeout is null : " + kafkaConsumerConfigId);
                        }

                        kafkaToMongoSinkTarget = new KafkaToMongoSinkTarget(kafkaConsumerConfigId, kafkaConsumer, pollingTimeout);
                        this.kafkaToMongoSinkTargets.put(kafkaConsumerConfigId, kafkaToMongoSinkTarget);
                    }

                    String sourceTopic = kafkaToMongoSinkConfig.getSourceTopic();
                    Map<String, MongoTargets> mongoTargetsMap = kafkaToMongoSinkTarget.getMongoTargetsMap();

                    if (mongoTargetsMap.containsKey(sourceTopic)) {
                        log.error("[init] Duplicate source topic : kafkaConsumerConfigId={} sourceTopic={}", kafkaConsumerConfigId, sourceTopic);
                        throw new InitException(
                                "Duplicate source topic : kafkaConsumerConfigId=" + kafkaConsumerConfigId + " sourceTopic=" + sourceTopic);
                    } else {
                        Integer defaultTargetConfigId = kafkaToMongoSinkConfig.getDefaultTargetConfigId();
                        MongoTarget defaultTarget = this.mongoTargetRegistry.getMongoTarget(defaultTargetConfigId);
                        MongoTargets mongoTargets = new MongoTargets(defaultTarget);

                        for (Integer mongoTargetConfigId : kafkaToMongoSinkConfig.getMongoTargetConfigIds()) {
                            MongoTarget mongoTarget = this.mongoTargetRegistry.getMongoTarget(mongoTargetConfigId);
                            if (mongoTarget == null) {
                                log.error("[init] mongoTarget is null : {}", mongoTargetConfigId);
                                throw new InitException("mongoTarget is null : " + mongoTargetConfigId);
                            } else {
                                mongoTargets.add(mongoTarget);
                            }
                        }

                        mongoTargetsMap.put(sourceTopic, mongoTargets);
                    }
                }
            } catch (JsonProcessingException e) {
                log.error("[init] [exception] Failed to parse kafkaToMongoSinkConfig", e);
                throw new InitException("Failed to parse kafkaToMongoSinkConfig: " + e.getMessage(), e);
            }
        }

        for (Map.Entry<Integer, KafkaToMongoSinkTarget> kafkaToMongoSinkTargetsEntry : this.kafkaToMongoSinkTargets.entrySet()) {
            Integer kafkaConsumerConfigId = kafkaToMongoSinkTargetsEntry.getKey();
            KafkaToMongoSinkTarget kafkaToMongoSinkTarget = kafkaToMongoSinkTargetsEntry.getValue();
            KafkaConsumer<UniversalSerdeData, UniversalSerdeData> kafkaConsumer
                    = kafkaToMongoSinkTarget.getKafkaConsumer();

            for (Map.Entry<String, MongoTargets> mongoTargetsEntry : kafkaToMongoSinkTarget.getMongoTargetsMap().entrySet()) {
                String topicName = mongoTargetsEntry.getKey();
                log.debug("[init] kafkaConsumerConfigId={} topicName={}", kafkaConsumerConfigId, topicName);

                MongoTargets mongoTargets = mongoTargetsEntry.getValue();
                for (MongoTarget mongoTarget : mongoTargets) {
                    Integer mongoConfigId = mongoTarget.getMongoClientConfigId();
                    String databaseName = mongoTarget.getDatabaseName();
                    String targetCollection = mongoTarget.getTargetCollection();
                    log.debug("[init] mongoConfigId={} databaseName={} targetCollection={}", mongoConfigId, databaseName, targetCollection);
                }

                List<String> topics = kafkaToMongoSinkTarget.getTopics();
                kafkaConsumer.subscribe(topics);
                log.debug("[init] Subscribing kafkaConsumerConfigId={} topics={}", kafkaConsumerConfigId, topics);
            }
        }
    }

    @Override
    public void run() {
        if (!this.kafkaToMongoSinkTargets.isEmpty() && this.isRunning.compareAndSet(false, true)) {
            try {
                Span span = this.tracer.spanBuilder(this.friendlyName + ".run").startSpan();
                long spanStartTime = Instant.now().toEpochMilli();

                try (Scope ignoredRunScope = span.makeCurrent()) {
                    log.debug("[run] Starting {}", this.friendlyName);

                    for (KafkaToMongoSinkTarget kafkaToMongoSinkTarget : this.kafkaToMongoSinkTargets.values()) {
                        try {
                            processKafkaToMongoSinkTarget(kafkaToMongoSinkTarget, span);
                        } catch (Exception e) {
                            span.recordException(e);
                            this.metricService.incrementCounter(FieldConstants.KAFKA_TO_MONGO_SINK_CONSUMER_ERROR_COUNT,
                                    Collections.singletonMap("consumerConfigId",
                                            String.valueOf(kafkaToMongoSinkTarget.getKafkaConsumerConfigId())), 1L);
                            log.error("[run] [exception] Failed processing KafkaToMongoSinkTarget :: id={} error={}",
                                    kafkaToMongoSinkTarget.getKafkaConsumerConfigId(), e.getMessage(), e);
                        }
                    }
                } catch (Exception e) {
                    span.recordException(e);
                    span.setAttribute(FieldConstants.ERROR_OCCURRED_KEY, true);
                    span.setAttribute(FieldConstants.ERROR_MESSAGE_KEY, e.getMessage());
                    this.metricService.incrementCounter(FieldConstants.KAFKA_TO_MONGO_SINK_ERROR_COUNT,
                            Collections.emptyMap(), 1L);
                    throw e;
                } finally {
                    long duration = Instant.now().toEpochMilli() - spanStartTime;
                    this.metricService.recordHistogram(FieldConstants.KAFKA_TO_MONGO_SINK_RUNNING_DURATION,
                            Collections.emptyMap(), duration);
                    span.setAttribute(FieldConstants.DURATION_KEY, duration);
                    span.end();
                }
            } finally {
                this.isRunning.set(false);
            }
        } else {
            log.trace("[run] Task still running, skipping execution.");
        }
    }

    void processKafkaToMongoSinkTarget(KafkaToMongoSinkTarget kafkaToMongoSinkTarget, Span span) {
        Integer kafkaConsumerConfigId = kafkaToMongoSinkTarget.getKafkaConsumerConfigId();

        span.addEvent("Polling target :: " + kafkaConsumerConfigId);
        log.debug("[run] Polling target :: {}", kafkaConsumerConfigId);

        ConsumerRecords<UniversalSerdeData, UniversalSerdeData> consumerRecords =
                kafkaToMongoSinkTarget.poll();

        Map<String, MongoTargets> mongoTargetsMap = kafkaToMongoSinkTarget.getMongoTargetsMap();

        int totalPolledRecords = consumerRecords.count();
        if (totalPolledRecords > 0) {
            this.metricService.incrementCounter(FieldConstants.KAFKA_TO_MONGO_SINK_FETCHED_RECORDS,
                    Collections.emptyMap(), (long) totalPolledRecords);
        }

        for (Map.Entry<String, MongoTargets> entry : mongoTargetsMap.entrySet()) {
            String sourceTopicName = entry.getKey();
            MongoTargets mongoTargets = entry.getValue();

            span.addEvent("Processing sourceTopicName :: " + sourceTopicName);
            log.debug("[run] Processing sourceTopicName :: {}", sourceTopicName);

            long topicStartTime = Instant.now().toEpochMilli();
            Map<String, String> topicTags = Map.of(FieldConstants.TOPIC, sourceTopicName);

            Iterable<ConsumerRecord<UniversalSerdeData, UniversalSerdeData>> topicIterable =
                    consumerRecords.records(sourceTopicName);

            KafkaRecordProcessingResult result = this.kafkaRecordProcessor
                    .processRecords(sourceTopicName, topicIterable, mongoTargets, this.friendlyName);

            try {
                List<KafkaWrapper> kafkaWrappers = result.getKafkaWrappers();
                if (!kafkaWrappers.isEmpty()) {
                    this.mongoSaver.saveKafkaWrappers(kafkaWrappers);
                }

                Map<Integer, List<MessageWrapper>> messageWrappersMap = result.getMessageWrappersMap();
                if (!messageWrappersMap.isEmpty()) {
                    this.mongoSaver.saveMessageWrappers(messageWrappersMap, this.friendlyName);
                }

                Map<TopicPartition, OffsetAndMetadata> highestOffsets = result.getHighestOffsets();
                kafkaToMongoSinkTarget.getKafkaConsumer().commitSync(highestOffsets);

                for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : highestOffsets.entrySet()) {
                    log.debug("[run] Committed offset :: {}[{}]",
                            offsetEntry.getKey(), offsetEntry.getValue().offset());
                }

                long duration = Instant.now().toEpochMilli() - topicStartTime;
                this.metricService.recordHistogram(FieldConstants.KAFKA_TO_MONGO_SINK_TOPIC_DURATION,
                        topicTags, duration);

                long processedCount = kafkaWrappers.size();
                for (List<MessageWrapper> wrappers : messageWrappersMap.values()) {
                    processedCount += wrappers.size();
                }
                if (processedCount > 0) {
                    this.metricService.incrementCounter(FieldConstants.KAFKA_TO_MONGO_SINK_RECORDS_COUNT,
                            topicTags, processedCount);
                }

            } catch (Exception e) {
                span.recordException(e);
                this.metricService.incrementCounter(FieldConstants.KAFKA_TO_MONGO_SINK_TOPIC_ERROR_COUNT,
                        topicTags, 1L);
                log.error("[run] [exception] Error saving/committing for topic={} error={}",
                        sourceTopicName, e.getMessage(), e);
            }
        }
    }

    @PreDestroy
    @Override
    public void teardown() {
        log.debug("[teardown] Stopping {}", friendlyName);
    }
}
