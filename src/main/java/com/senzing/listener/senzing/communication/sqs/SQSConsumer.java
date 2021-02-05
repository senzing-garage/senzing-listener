package com.senzing.listener.senzing.communication.sqs;

import java.io.StringReader;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.senzing.listener.senzing.communication.MessageConsumer;
import com.senzing.listener.senzing.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.senzing.data.ConsumerCommandOptions;
import com.senzing.listener.senzing.service.ListenerService;
import com.senzing.listener.senzing.service.exception.ServiceExecutionException;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * A consumer for SQS.
 */
public class SQSConsumer implements MessageConsumer {

  private String queueName;

  ListenerService service;

  private SqsClient sqsClient;

  /**
   * Wait parameter in seconds to SQS in case no messages are waiting to be collected.
   */
  private static final int SQS_WAIT_SECS = 10;


  /**
   * Generates a SQS consumer.
   * 
   * @return
   * 
   * @throws MessageConsumerSetupException
   */
  public static SQSConsumer generateSQSConsumer() {
    return new SQSConsumer();
  }

  private SQSConsumer() {
  }

  /**
   * Initializes the object. It sets the object up based on configuration passed in.
   * 
   * @param config Configuration string containing the needed information to connect to SQS.
   * The configuration is in JSON format:
   * {
   *   "queueName":"<URL>"              # required value
   * }
   *
   * @throws MessageConsumerSetupException
   */
  public void init(String config) throws MessageConsumerSetupException {
    try {
      JsonReader reader = Json.createReader(new StringReader(config));
      JsonObject configObject = reader.readObject();
      queueName = getConfigValue(configObject, ConsumerCommandOptions.MQ_QUEUE, true);
      sqsClient = SqsClient.builder()
        .build();
    } catch (RuntimeException e) {
      throw new MessageConsumerSetupException(e);
    }
  }

  /**
   * Sets up a SQS consumer and then receives messages from SQS and
   * feeds to service.
   * 
   * @param service Processes messages
   * 
   * @throws MessageConsumerSetupException
   */
  @Override
  public void consume(ListenerService service) throws MessageConsumerSetupException {

    this.service = service;

    while (true) {
      try {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
          .queueUrl(queueName)
          .waitTimeSeconds(SQS_WAIT_SECS)
          .build();
        ReceiveMessageResponse messageResponse = sqsClient.receiveMessage(receiveMessageRequest);
        if (!messageResponse.sdkHttpResponse().isSuccessful())
          throw new MessageConsumerSetupException(String.valueOf(messageResponse.sdkHttpResponse().statusCode()));

        List<Message> messages = messageResponse.messages();
        for (Message message: messages) {
          processMessage(message.body());
          deleteSqsMessage(message.receiptHandle());
        }
      } catch (SdkException e) {
        throw new MessageConsumerSetupException(e);
      }
    }
  }

  private void processMessage(String message) {
    try {
      service.process(message);
    } catch (ServiceExecutionException e) {
      e.printStackTrace();
    }
  }

  private void deleteSqsMessage(String handle) {
    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
            .queueUrl(queueName)
            .receiptHandle(handle)
            .build();
    sqsClient.deleteMessage(deleteMessageRequest);
}

  private String getConfigValue(JsonObject configObject, String key, boolean required) throws MessageConsumerSetupException {
    String configValue = configObject.getString(key, null);
    if (required && (configValue == null || configValue.isEmpty())) {
      StringBuilder message = new StringBuilder("Following configuration parameter missing: ").append(key);
      throw new MessageConsumerSetupException(message.toString());
    }
    return configValue;
  }
}
