package data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

/**
 * Created by bvenkatesan on 12/13/16.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaTopicList {
    @JsonProperty(value = "topics")
    List<KafkaTopic> topicList = Lists.newArrayList();
}
