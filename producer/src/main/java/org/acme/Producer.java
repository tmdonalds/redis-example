package org.acme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;


@ApplicationScoped
public class Producer {
  @Inject
  ObjectMapper objectMapper;

  @Inject
  SqsClient sqsClient;

  @Inject
  Vertx vertx;

  @Inject
  Logger logger;


  public void processMessages() throws JsonProcessingException {
    String queueUrl = getQueueUrl("queueurl");
    List<Message> messages;

    do {
      messages = getQueueMessage(queueUrl, 60);
      if (!messages.isEmpty()) {
        DatasourceRequest datasourceRequest = objectMapper.readValue(messages.get(0).body(), DatasourceRequest.class);
        sendMessage(datasourceRequest);
        logger.info("Completed writing message");
      }

    } while (!messages.isEmpty());

    logger.info("finished");
  }

  private void sendMessage(DatasourceRequest datasourceRequest)  {

    final String connectionString =
        String.format("redis://%s:%s", datasourceRequest.redisHost(), datasourceRequest.redisPort().toString());
    logger.infof("Connecting to %s", connectionString);

    final Redis client = Redis.createClient(vertx, connectionString);

    client
        .connect()
        .onSuccess(conn -> conn.send(
                Request.cmd(Command.XADD).arg(datasourceRequest.uniqueIdentifier()).arg("*").arg("payload").arg(datasourceRequest.message()))
            .onSuccess(
                response -> {
                  logger.infof("Message successfully written to redis with id %s",
                      response.toString());
                  conn.close();
                  client.close();
                })
            .onFailure(e -> logger.error("Failed to write message to redis", e)))
        .onFailure(logger::error);
  }

  private String getQueueUrl(String queue) {

    GetQueueUrlRequest queueUrlRequest =
        GetQueueUrlRequest.builder()
            .queueName(queue)
            .build();
    //Get url of queue
    GetQueueUrlResponse queueUrlResponse = this.sqsClient.getQueueUrl(queueUrlRequest);

    return queueUrlResponse.queueUrl();
  }

  private List<Message> getQueueMessage(String queueUrl, int visibilityTimeout) {
    ReceiveMessageRequest requestQueryMessage =
        ReceiveMessageRequest.builder()
            .visibilityTimeout(visibilityTimeout)//time to hide this queue from any other process
            .queueUrl(queueUrl)
            .maxNumberOfMessages(1)//messages at a time
            .build();
    return this.sqsClient.receiveMessage(requestQueryMessage).messages();
  }
}
