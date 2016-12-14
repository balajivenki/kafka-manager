package data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;

import java.util.HashMap;

/**
 * Created by bvenkatesan on 12/13/16.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaTopic {
    @NonNull
    private String topicName;
    private int partitions;
    private int replication;
    private TopicAction topicAction;

    @JsonProperty(value = "properties")
    private HashMap<String, String> topicProperties;

}
