package manager;

import data.KafkaTopic;
import data.KafkaTopicList;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import lombok.Data;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by bvenkatesan on 12/13/16.
 */
@Data
public class KafkaTopicsProcessor {

    private String zooKeeperHostList;

    private int sessionTimeoutMs = 10 * 1000;
    private int connectionTimeoutMs = 8 * 1000;

    private KafkaTopicList kafkaTopicList;

    private ZkClient zkClient;

    private ZkUtils zkUtils;

    public KafkaTopicsProcessor(String zooKeeperHost, KafkaTopicList kafkaTopicList) {
        this.zooKeeperHostList = zooKeeperHost;
        this.kafkaTopicList = kafkaTopicList;
    }

    private ZkClient getZooKeeperClient() {
        return new ZkClient(
                zooKeeperHostList,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
    }

    public void process() {
        ZkClient zkClient = getZooKeeperClient();

        // Security for Kafka was added in Kafka 0.9.0.0
        boolean isSecureKafkaCluster = false;
        // ZkUtils for Kafka was used in Kafka 0.9.0.0 for the AdminUtils API
        zkUtils = new ZkUtils(zkClient, new ZkConnection(zooKeeperHostList), isSecureKafkaCluster);

        kafkaTopicList.getTopicList().stream()
                .forEach(kafkaTopic -> processTopic(kafkaTopic));

        zkClient.close();
    }

    private void processTopic(KafkaTopic topic) {
        boolean topicExist = AdminUtils.topicExists(zkUtils, topic.getTopicName());
        System.out.println(topic.getTopicName() + ", " + topicExist);

        switch (topic.getTopicAction()) {
            case CREATE: {
                if (topicExist) {
                    System.out.println("Topic " + topic.getTopicName() + " already exist so skipping CREATE.");
                } else {
                    AdminUtils.createTopic(zkUtils, topic.getTopicName(), topic.getPartitions(), topic.getReplication(), buildTopicProperties(topic.getTopicProperties()));
                    System.out.println("Topic " + topic.getTopicName() + " created");
                }

                break;
            }

            case DELETE: {
                if (!topicExist) {
                    System.out.println("Topic " + topic.getTopicName() + " does not exist so skipping DELETE.");
                } else {
                    AdminUtils.deleteTopic(zkUtils, topic.getTopicName());
                    System.out.println("Topic " + topic.getTopicName() + " deleted");
                }
                break;
            }

            default: {
                System.out.println("Invalid topic action: " + topic.getTopicAction());
                break;
            }
        }

    }

    private Properties buildTopicProperties(HashMap<String, String> props) {
        Properties topicProperties = new Properties();

        props.forEach((key, value) -> {
            topicProperties.setProperty(key, value);
        });

        return topicProperties;
    }
}
