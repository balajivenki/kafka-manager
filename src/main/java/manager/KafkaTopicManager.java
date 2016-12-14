package manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import data.KafkaTopicList;
import lombok.Cleanup;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;

/**
 * Created by bvenkatesan on 12/13/16.
 */
public class KafkaTopicManager {

    private static String DEFAULT_CONF_FILE = "kafka-topic.yml";

    public static void main(String[] args) throws Exception {
        System.out.println("args:" + args[0]);

/*        String zookeeperConnect = "qzkpr01.p08.eng.qualys.com:50024/qkafka";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;

        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);

        // Security for Kafka was added in Kafka 0.9.0.0
        boolean isSecureKafkaCluster = false;
        // ZkUtils for Kafka was used in Kafka 0.9.0.0 for the AdminUtils API
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);*/


        //using the jackson yaml object mapper
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        @Cleanup InputStream inputStream = null;
        String filePath = null;


        try {
            Optional<String> optionalFilePath = Optional.ofNullable(filePath);

            if (optionalFilePath.isPresent()) {
                inputStream = new FileInputStream(new File(optionalFilePath.get()));
            } else {
                inputStream = KafkaTopicsProcessor.class.getClassLoader().getResourceAsStream(DEFAULT_CONF_FILE);
            }
        } catch (Exception e) {
            throw new Exception("Error while loading loading conf file");
        }

        Optional<InputStream> inputStreamOptional = Optional.ofNullable(inputStream);
        KafkaTopicList kafkaTopicList;

        if (inputStreamOptional.isPresent()) {
            kafkaTopicList = yamlObjectMapper.readValue(inputStreamOptional.get(), KafkaTopicList.class);
        } else {
            throw new Exception("Could not load the conf file");
        }

        KafkaTopicsProcessor kafkaTopicsProcessor = new KafkaTopicsProcessor(args[0].toString().trim(), kafkaTopicList);
        kafkaTopicsProcessor.process();

/*        String topic = "my-java-topic";
        int partitions = 2;
        int replication = 3;

        // Add topic configuration here
        Properties topicConfig = new Properties();
        topicConfig.setProperty("retention.ms", "10");
        topicConfig.setProperty("max.message.bytes", "20000");

//        retention.ms
//        max.message.bytes
//        segment.index.bytes
//        segment.bytes
//        min.cleanable.dirty.ratio
//        min.insync.replicas
//        delete.retention.ms
//        flush.messages
//        preallocate
//        retention.bytes
//        flush.ms
//        cleanup.policy
//        file.delete.delay.ms
//        segment.jitter.ms
//        index.interval.bytes
//        compression.type
//        segment.ms
//        unclean.leader.election.enable

        if(!AdminUtils.topicExists(zkUtils, topic)) {
            AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
        } else {
            throw new Exception("Topic already exist");
        }
        zkClient.close();*/
    }
}
