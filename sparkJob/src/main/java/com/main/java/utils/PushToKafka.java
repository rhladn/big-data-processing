package com.main.java.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.shade.org.apache.zookeeper.server.ExitCode;

import java.io.Serializable;

import static com.main.java.constants.Constants.DELIMITOR;

/**
 * Created by rahul.tandon
 * Publishes key:userId and value:EntityRelevance Object converted to json to kafka
 * EntityRelevance Object consists of List<Inference> which is equivalent to List<cohortId>
 */
@Slf4j
public class PushToKafka implements Serializable {

    private static PushToKafka userCohortPushToKafka = null;
    private static PushToKafka<String, String> pushToKafka;
    private static final Priority priority = Priority.USERCOHORT;
    private static final String semanticType = "UC";

    private PushToKafka() {
        pushToKafka = KafkaPublisherFactory
                .getKafkaPublisher(SerializerType.STRING, BD, KafkaConfigProvider.getKafkaConfig(BD), RecoMetricRegistry.getInstance(PHOENIX));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Terminating Kafka Producer!!!");
            pushToKafka.close();
        }));
    }

    public static PushToKafka getInstance() {
        if (userCohortPushToKafka == null) {
            synchronized (PushToKafka.class) {
                if(userCohortPushToKafka == null) {
                    userCohortPushToKafka = new PushToKafka();
                }
            }
        }
        return userCohortPushToKafka;
    }

    /**
     * @param userId          this is the string of user Id which is passed as key to the kafka
     * @param entityRelevance this is the string form of the entityRelevance which is passed as value to kafka
     */
    public boolean push(String userId, String entityRelevance) {
        try {
            final String payload = semanticType + DELIMITOR + userId + DELIMITOR + entityRelevance;
            ExitCode kafkaPublishExitCode = pushToKafka.publish(priority.toString(), payload);
            return !kafkaPublishExitCode.equals(FAILURE);
        } catch (Exception e) {
            throw new RuntimeException("Error publishing user cohort data to bd-kafka " + e.toString());
        }
    }
}
