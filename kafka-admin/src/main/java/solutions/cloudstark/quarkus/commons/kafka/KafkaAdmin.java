/*
 *    Copyright 2020 SMB GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package solutions.cloudstark.quarkus.commons.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;
import solutions.cloudstark.quarkus.commons.kafka.annotations.KafkaAdminConfig;

@Slf4j
@ApplicationScoped
public class KafkaAdmin {

  private final Map<String, Object> config;
  private final List<NewTopic> topics;

  @Inject
  public KafkaAdmin(
      @NotNull @KafkaAdminConfig Map<String, Object> config, @Any Instance<NewTopic> topics) {
    this.config = config;
    this.topics = topics.stream().collect(Collectors.toList());
  }

  void onStart(@Observes StartupEvent event) {
    if (topics.isEmpty()) {
      return;
    }
    AdminClient adminClient = AdminClient.create(config);

    Map<String, NewTopic> topicNameToTopic = new HashMap<>();
    topics.forEach(t -> topicNameToTopic.compute(t.name(), (k, v) -> t));
    DescribeTopicsResult topicInfo =
        adminClient.describeTopics(
            topics.stream().map(NewTopic::name).collect(Collectors.toList()));
    List<NewTopic> topicsToAdd = new ArrayList<>();
    Map<String, NewPartitions> topicsToModify =
        checkPartitions(topicNameToTopic, topicInfo, topicsToAdd);
    if (topicsToAdd.size() > 0) {
      addTopics(adminClient, topicsToAdd);
    }
    if (topicsToModify.size() > 0) {
      modifyTopics(adminClient, topicsToModify);
    }
  }

  private Map<String, NewPartitions> checkPartitions(
      Map<String, NewTopic> topicNameToTopic,
      DescribeTopicsResult topicInfo,
      List<NewTopic> topicsToAdd) {
    Map<String, NewPartitions> topicsToModify = new HashMap<>();
    topicInfo
        .values()
        .forEach(
            (n, f) -> {
              NewTopic topic = topicNameToTopic.get(n);
              try {
                TopicDescription topicDescription = f.get(30, TimeUnit.SECONDS);
                if (topic.numPartitions() < topicDescription.partitions().size()) {
                  log.info(
                      String.format(
                          "Topic '%s' exists but has a different partition count: %d not %d",
                          n, topicDescription.partitions().size(), topic.numPartitions()));
                } else if (topic.numPartitions() > topicDescription.partitions().size()) {
                  log.info(
                      String.format(
                          "Topic '%s' exists but has a different partition count: %d not %d, increasing "
                              + "if the broker supports it",
                          n, topicDescription.partitions().size(), topic.numPartitions()));
                  topicsToModify.put(n, NewPartitions.increaseTo(topic.numPartitions()));
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (TimeoutException e) {
                throw new KafkaException("Timed out waiting to get existing topics", e);
              } catch (ExecutionException e) {
                topicsToAdd.add(topic);
              }
            });
    return topicsToModify;
  }

  private void modifyTopics(AdminClient adminClient, Map<String, NewPartitions> topicsToModify) {
    CreatePartitionsResult partitionsResult = adminClient.createPartitions(topicsToModify);
    try {
      partitionsResult.all().get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting for partition creation results", e);
    } catch (TimeoutException e) {
      throw new KafkaException("Timed out waiting for create partitions results", e);
    } catch (ExecutionException e) {
      if (e.getCause()
          instanceof InvalidPartitionsException) { // Possible race with another app instance
        log.debug("Failed to create partitions", e.getCause());
      } else {
        log.error("Failed to create partitions", e.getCause());
        if (!(e.getCause() instanceof UnsupportedVersionException)) {
          throw new KafkaException("Failed to create partitions", e.getCause()); // NOSONAR
        }
      }
    }
  }

  private void addTopics(AdminClient adminClient, List<NewTopic> topicsToAdd) {
    CreateTopicsResult topicResults = adminClient.createTopics(topicsToAdd);
    try {
      topicResults.all().get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting for topic creation results", e);
    } catch (TimeoutException e) {
      throw new KafkaException("Timed out waiting for create topics results", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) { // Possible race with another app instance
        log.debug("Failed to create topics", e.getCause());
      } else {
        log.error("Failed to create topics", e.getCause());
        throw new KafkaException("Failed to create topics", e.getCause()); // NOSONAR
      }
    }
  }
}
