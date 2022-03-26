package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.service.ListenerService;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.util.JsonUtilities;

import javax.json.*;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.util.*;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.senzing.listener.communication.MessageConsumer.State.CONSUMING;
import static com.senzing.listener.communication.AbstractMessageConsumer.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.util.JsonUtilities.*;

/**
 * Tests for {@link AbstractMessageConsumer}.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class AbstractMessageConsumerTest {
  private static SecureRandom PRNG = new SecureRandom();
  static {
    double value = PRNG.nextDouble();
  }
  private Set<Long> prevousAffectedSet = null;
  private int noOverlapCount = 0;

  private Set<Long> getAffectedSet(int minEntityId,
                                  int maxEntityId,
                                  int maxAffected)
  {
    int idSpread = (maxEntityId - minEntityId);
    Set<Long> affectedSet = new LinkedHashSet<>();
    int affectedCount = Math.max(1, PRNG.nextInt(maxAffected));
    for (int index2 = 0; index2 < affectedCount; index2++) {
      long entityId = ((long) (minEntityId + PRNG.nextInt(idSpread)));
      entityId = Math.min(entityId,  (long) maxEntityId);
      affectedSet.add(entityId);
    }

    synchronized (this) {
      if (prevousAffectedSet != null) {
        boolean overlap = false;
        for (Long entityId : affectedSet) {
          if (prevousAffectedSet.contains(entityId)) {
            overlap = true;
            break;
          }
        }
        noOverlapCount = (overlap) ? 0 : (noOverlapCount+1);

        // check if we have had no contention in a while and force it if not
        if (noOverlapCount > 20) {
          // check if max size
          if (affectedSet.size() == maxAffected) {
            // remove the first if so
            affectedSet.remove(affectedSet.iterator().next());
          }
          // then add one from the previous to create overlap
          affectedSet.add(prevousAffectedSet.iterator().next());
          noOverlapCount = 0;
        }
      }

      // set the previous and return
      prevousAffectedSet = affectedSet;
    }
    return Collections.unmodifiableSet(affectedSet);
  }

  private String getRecordId(int nextRecordId) {
    return "RECORD-" + nextRecordId;
  }

  private String getDataSource(List<String> dataSources) {
    int index = PRNG.nextInt(dataSources.size());
    index = Math.min(Math.max(0, index), dataSources.size() - 1);
    return dataSources.get(index);
  }

  public int buildInfoBatches(List<Message> messageList,
                              int           batchCount,
                              List<String>  dataSources,
                              int           minBatchSize,
                              int           maxBatchSize,
                              int           minEntityId,
                              int           maxEntityid,
                              int           maxAffected,
                              double        failureRate)
  {
    // fabricate record IDs
    int nextRecordId = (int) Math.pow(
        10, (Math.floor(Math.log10(batchCount * maxBatchSize)) + 1));
    int count = 0;
    // create the result list
    for (int index = 0; index < batchCount; index++) {
      boolean failure = PRNG.nextDouble() < failureRate;
      int failureCount = 0;
      if (failure) {
        failureCount = PRNG.nextInt(3);
      }
      // determine the batch size
      int batchSize = Math.max(1, minBatchSize + PRNG.nextInt(maxBatchSize));
      int messageId = nextRecordId;
      if (batchSize == 1) {
        count++;
        JsonObjectBuilder job = Json.createObjectBuilder();
        buildInfoMessage(job,
                         messageId,
                         failureCount,
                         null,
                         getDataSource(dataSources),
                         getRecordId(nextRecordId++),
                         getAffectedSet(minEntityId, maxEntityid, maxAffected));
        JsonObject jsonObject = job.build();
        String messageText = toJsonText(jsonObject);
        messageList.add(new Message(messageId, messageText));

      } else {
        count += batchSize;
        JsonArrayBuilder jab = Json.createArrayBuilder();
        nextRecordId = buildInfoBatch(jab,
                                      batchSize,
                                      dataSources,
                                      nextRecordId,
                                      failureCount,
                                      minEntityId,
                                      maxEntityid,
                                      maxAffected);
        JsonArray jsonArray = jab.build();
        String messageText = toJsonText(jsonArray);
        messageList.add(new Message(messageId, messageText));
      }
    }
    return count;
  }

  public Message buildInfoBatch(int               batchSize,
                                List<String>      dataSources,
                                int               nextRecordId,
                                int               maxFailureCount,
                                int               minEntityId,
                                int               maxEntityId,
                                int               maxAffected)
  {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    int messageId = nextRecordId;
    buildInfoBatch(jab,
                   batchSize,
                   dataSources,
                   nextRecordId,
                   maxFailureCount,
                   minEntityId,
                   maxEntityId,
                   maxAffected);
    JsonArray jsonArray = jab.build();
    String messageText = toJsonText(jsonArray);
    return new Message(messageId, messageText);
  }

  public int buildInfoBatch(JsonArrayBuilder    builder,
                            int                 batchSize,
                            List<String>        dataSources,
                            int                 nextRecordId,
                            int                 maxFailureCount,
                            int                 minEntityId,
                            int                 maxEntityId,
                            int                 maxAffected)
  {
    int messageId = nextRecordId; // all in the batch belong to same message
    for (int index1 = 0; index1 < batchSize; index1++) {
      JsonObjectBuilder job = Json.createObjectBuilder();
      int failureCount = (maxFailureCount == 0)
          ? 0 : PRNG.nextInt(maxFailureCount);
      buildInfoMessage(job,
                       messageId,
                       failureCount,
                       null,
                       getDataSource(dataSources),
                       getRecordId(nextRecordId++),
                       getAffectedSet(minEntityId, maxEntityId, maxAffected));
      builder.add(job);
    }
    return nextRecordId;
  }

  public String buildInfoMessage(int                messageId,
                                 String             dataSource,
                                 String             recordId,
                                 long...            affectedEntityIds)
  {
    return buildInfoMessage(messageId,
                            0,
                            null,
                            dataSource,
                            recordId,
                            affectedEntityIds);
  }

  public String buildInfoMessage(int                messageId,
                                 int                failureCount,
                                 Long               processingTime,
                                 String             dataSource,
                                 String             recordId,
                                 long...            affectedEntityIds)
  {
    JsonObjectBuilder job = Json.createObjectBuilder();
    buildInfoMessage(job,
                     messageId,
                     failureCount,
                     processingTime,
                     dataSource,
                     recordId,
                     affectedEntityIds);
    JsonObject jsonObject = job.build();
    return JsonUtilities.toJsonText(jsonObject);
  }

  public String buildInfoMessage(int                messageId,
                                 String             dataSource,
                                 String             recordId,
                                 Set<Long>          affectedEntityIds)
  {
    return buildInfoMessage(messageId,
                            0,
                            null,
                            dataSource,
                            recordId,
                            affectedEntityIds);
  }

  public String buildInfoMessage(int                messageId,
                                 int                failureCount,
                                 Long               processingTime,
                                 String             dataSource,
                                 String             recordId,
                                 Set<Long>          affectedEntityIds)
  {
    JsonObjectBuilder job = Json.createObjectBuilder();
    buildInfoMessage(job,
                     messageId,
                     failureCount,
                     processingTime,
                     dataSource,
                     recordId,
                     affectedEntityIds);
    JsonObject jsonObject = job.build();
    return JsonUtilities.toJsonText(jsonObject);
  }

  public void buildInfoMessage(JsonObjectBuilder  builder,
                               int                messageId,
                               String             dataSource,
                               String             recordId,
                               long...            affectedEntityIds)
  {
    this.buildInfoMessage(builder,
                          messageId,
                          0,
                          null,
                          dataSource,
                          recordId,
                          affectedEntityIds);
  }

  public void buildInfoMessage(JsonObjectBuilder  builder,
                               int                messageId,
                               int                failureCount,
                               Long               processTime,
                               String             dataSource,
                               String             recordId,
                               long...            affectedEntityIds)
  {
    Set<Long> affectedSet = new LinkedHashSet<>();
    for (long entityId : affectedEntityIds) {
      affectedSet.add(entityId);
    }
    this.buildInfoMessage(builder,
                          messageId,
                          failureCount,
                          processTime,
                          dataSource,
                          recordId,
                          affectedSet);
  }

  public void buildInfoMessage(JsonObjectBuilder  builder,
                               int                messageId,
                               String             dataSource,
                               String             recordId,
                               Set<Long>          affectedEntityIds)
  {
    buildInfoMessage(builder,
                     messageId,
                     0,
                     null,
                     dataSource,
                     recordId,
                     affectedEntityIds);
  }

  public void buildInfoMessage(JsonObjectBuilder  builder,
                               int                messageId,
                               int                failureCount,
                               Long               processTime,
                               String             dataSource,
                               String             recordId,
                               Set<Long>          affectedEntityIds)
  {
    builder.add("MESSAGE_ID", messageId);
    if (failureCount > 0) {
      builder.add("FAILURE_COUNT", failureCount);
    }
    if (processTime != null) {
      builder.add("PROCESSING_TIME", processTime);
    }
    builder.add("DATA_SOURCE", dataSource);
    builder.add("RECORD_ID", recordId);
    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (long entityId: affectedEntityIds) {
      JsonObjectBuilder job2 = Json.createObjectBuilder();
      job2.add("ENTITY_ID", entityId);
      job2.add("LENS_CODE", "DEFAULT");
      jab.add(job2);
    }
    builder.add("AFFECTED_ENTITIES", jab);
  }

  public static class Message {
    private int id;
    private String body;
    private Long processingTime = null;
    public Message(int id, String msgText) {
      this(id, null, msgText);
    }
    public Message(int id, Long processingTime, String msgText) {
      this.id = id;
      this.body = msgText;
      this.processingTime = processingTime;
    }
    public int getId() {
      return this.id;
    }
    public String getBody() {
      return this.body;
    }
    public String toString() {
      return "Message (" + this.getId() + "): " + this.getBody();
    }

  }

  public static class TestMessageConsumer
      extends AbstractMessageConsumer<Message>
  {
    private List<Message> messageQueue = new LinkedList<>();
    private IdentityHashMap<Message,Long> dequeuedMap = new IdentityHashMap<>();
    private Thread consumptionThread = null;
    private int dequeueCount;
    private long dequeueSleep;
    private long visibilityTimeout;
    private long expectedFailureCount = 0L;
    private long expectedMessageRetryCount = 0L;
    private long expectedInfoMessageRetryCount = 0L;
    public TestMessageConsumer(int            dequeueCount,
                               long           dequeueSleep,
                               long           visibilityTimeout,
                               List<Message>  messages)
    {
      this.dequeueCount       = dequeueCount;
      this.dequeueSleep       = dequeueSleep;
      this.visibilityTimeout  = visibilityTimeout;
      for (Message message: messages) {
        this.messageQueue.add(message);
        String body = message.getBody().trim();
        List<JsonObject> jsonObjects = new ArrayList<>();
        if (body.startsWith("[")) {
          JsonArray jsonArray = parseJsonArray(body);
          for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class))
          {
            jsonObjects.add(jsonObject);
          }
        } else {
          jsonObjects.add(parseJsonObject(body));
        }
        int maxFailures = 0;
        for (JsonObject jsonObject : jsonObjects) {
          int failureCount = getInteger(jsonObject, "FAILURE_COUNT", 0);
          this.expectedFailureCount += failureCount;
          if (failureCount > maxFailures) {
            maxFailures = failureCount;
          }
        }
        this.expectedMessageRetryCount += maxFailures;
        this.expectedInfoMessageRetryCount += (maxFailures * jsonObjects.size());
      }

    }
    public int getDequeueCount() {
      return this.dequeueCount;
    }
    public long getDequeueSleep() {
      return this.dequeueSleep;
    }
    public long getVisibilityTimeout() {
      return this.visibilityTimeout;
    }
    public long getExpectedFailureCount() {
      return this.expectedFailureCount;
    }
    public long getExpectedMessageRetryCount() {
      return this.expectedMessageRetryCount;
    }
    public long getExpectedInfoMessageRetryCount() {
      return this.expectedInfoMessageRetryCount;
    }
    protected void doInit(JsonObject config) { }
    protected void doDestroy() {
      // join to the consumption thread
      try {
        this.consumptionThread.join();
        synchronized (this) {
          this.consumptionThread = null;
        }
      } catch (InterruptedException ignore) {
        // ignore
      }
    }
    protected void doConsume(ListenerService service) {
      this.consumptionThread = new Thread(() -> {
        long start = System.nanoTime() - 15000000000L;
        int timeoutCount = 0;
        int restoreCount = 0;
        while (this.getState() == CONSUMING) {
          long end = System.nanoTime();
          if (((end - start)/1000000L) > 10000L) {
            start = end;
            //if (timeoutCount > 0) {
            //  restoreCount += timeoutCount;
            //  System.err.println("RESTORED " + timeoutCount
            //                         + " MESSAGES DUE TO VISIBILITY TIMEOUT "
            //                      + "(" + restoreCount + " TOTAL)");
            //  timeoutCount = 0;
            //}
          }
          // dequeue messages
          for (int index = 0; index < this.dequeueCount; index++) {
            Message msg = null;
            synchronized (this.messageQueue) {
              if (this.messageQueue.size() == 0) break;
              msg = this.messageQueue.remove(0);
              long now = System.nanoTime() / 1000000L;
              this.dequeuedMap.put(msg, now);
            }
            this.enqueueMessages(service, msg);
          }

          // check for messages that have timed out and enqueue them again
          synchronized (this.messageQueue) {
            Iterator<Map.Entry<Message,Long>> iter
                = this.dequeuedMap.entrySet().iterator();
            while (iter.hasNext()) {
              Map.Entry<Message,Long> entry = iter.next();
              Message msg       = entry.getKey();
              Long    timestamp = entry.getValue();
              long now = System.nanoTime() / 1000000L;
              if (now - timestamp > this.visibilityTimeout) {
                iter.remove();
                timeoutCount++;
                this.messageQueue.add(0, msg);
              }
            }
          }

          // now sleep for a while
          try {
            Thread.sleep(this.dequeueSleep);
          } catch (InterruptedException ignore) {
            // ignore
          }
        }
        //if (timeoutCount > 0) {
        //  restoreCount += timeoutCount;
        //  System.err.println("RESTORED " + timeoutCount
        //                         + " MESSAGES DUE TO VISIBILITY TIMEOUT "
        //                         + "(" + restoreCount + " TOTAL)");
        // }
      });

      this.consumptionThread.start();
    }

    protected String extractMessageBody(Message msg) {
      return msg.getBody();
    }

    protected void disposeMessage(Message msg) {
      synchronized (this.messageQueue) {
        this.dequeuedMap.remove(msg);
      }
    }
  }

  public static class MessageCounts
      implements Cloneable, Comparable<MessageCounts>
  {
    private String messageText;
    private int beginCount = 0;
    private int successCount = 0;
    private int failureCount = 0;
    private long firstBeginTime = 0L;
    private long lastBeginTime = 0L;
    private long lastEndTime = 0L;
    private Integer messageId = null;
    public MessageCounts(String message) {
      this.messageText = message;
      try {
        JsonObject jsonObject = parseJsonObject(this.messageText);
        this.messageId = getInteger(
            jsonObject, "MESSAGE_ID", null);

      } catch (Exception e) {
        // allow for tests with bad JSON by having the MESSAGE_ID appear first
        // in a stand-alone JSON object
        try {
          String firstLine = (new BufferedReader(
              new StringReader(this.getMessageText()))).readLine();
          this.messageId = Integer.parseInt(firstLine.trim());
        } catch (Exception ignore) {
          // do nothing
        }
      }
    }
    public Object clone() {
      try {
        return super.clone();
      } catch (CloneNotSupportedException cannotHappen) {
        throw new IllegalStateException("Unexpected clone failure");
      }
    }
    public int hashCode() {
      synchronized (this) {
        return Objects.hash(
            this.getMessageId(),
            this.getFirstBeginTime(),
            this.getLastBeginTime(),
            this.getLastEndTime(),
            this.getBeginCount(),
            this.getSuccessCount(),
            this.getFailureCount());
      }
    }
    public boolean equals(Object that) {
      if (that == null) return false;
      if (this == that) return true;
      if (this.getClass() != that.getClass()) return false;
      MessageCounts counts = (MessageCounts) that;
      int thisPriority = System.identityHashCode(this);
      int thatPriority = System.identityHashCode(that);
      MessageCounts first = (thisPriority < thatPriority) ? this : counts;
      MessageCounts second = (thisPriority < thatPriority) ? counts : this;
      synchronized (first) {
        synchronized (second) {
          return Objects.equals(this.getMessageId(), counts.getMessageId())
              && Objects.equals(this.getMessageText(),
                                counts.getMessageText())
              && Objects.equals(this.getFirstBeginTime(),
                                counts.getFirstBeginTime())
              && Objects.equals(this.getLastBeginTime(),
                                counts.getLastBeginTime())
              && Objects.equals(this.getLastEndTime(),
                                counts.getLastEndTime())
              && Objects.equals(this.getBeginCount(),
                                counts.getBeginCount())
              && Objects.equals(this.getSuccessCount(),
                                counts.getSuccessCount())
              && Objects.equals(this.getFailureCount(),
                                counts.getFailureCount());
        }
      }
    }

    public int compareTo(MessageCounts that) {
      if (that == null) return 1;
      if (that == this) return 0;
      int thisPriority = System.identityHashCode(this);
      int thatPriority = System.identityHashCode(that);
      MessageCounts first = (thisPriority < thatPriority) ? this : that;
      MessageCounts second = (thisPriority < thatPriority) ? that : this;

      synchronized (first) {
        synchronized (second) {
          long diff = this.getLastBeginTime() - that.getLastBeginTime();
          if (diff != 0) return (diff < 0) ? -1 : 1;
          diff = this.getFirstBeginTime() - that.getFirstBeginTime();
          if (diff != 0) return (diff < 0) ? -1 : 1;
          diff = this.getLastEndTime() - that.getLastEndTime();
          if (diff != 0) return (diff < 0) ? -1 : 1;
          diff = (this.getMessageId() - that.getMessageId());
          if (diff != 0) return (diff < 0) ? -1 : 1;
          diff = (this.getBeginCount() - that.getBeginCount());
          if (diff != 0) return (diff < 0) ? -1 : 1;
          diff = (this.getSuccessCount() - that.getSuccessCount());
          if (diff != 0) return (diff < 0) ? -1 : 1;
          diff = (this.getFailureCount() - that.getFailureCount());
          if (diff != 0) return (diff < 0) ? -1 : 1;
          return (this.getMessageText().compareTo(that.getMessageText()));
        }
      }
    }
    public Integer getMessageId() {
      return this.messageId;
    }
    public synchronized void recordBegin() {
      this.beginCount++;
      long now = System.nanoTime();
      if (this.firstBeginTime == 0) this.firstBeginTime = now;
      this.lastBeginTime = now;
    }
    public synchronized void recordSuccess() {
      this.successCount++;
      this.lastEndTime = System.nanoTime();
    }
    public synchronized void recordFailure() {
      this.failureCount++;
      this.lastEndTime = System.nanoTime();
    }
    public String getMessageText() { return this.messageText; }
    public synchronized int getBeginCount() { return this.beginCount; }
    public synchronized int getSuccessCount() { return this.successCount; }
    public synchronized int getFailureCount() { return this.failureCount; }
    public synchronized long getFirstBeginTime() { return this.firstBeginTime; }
    public synchronized long getLastBeginTime() { return this.lastBeginTime; }
    public synchronized long getLastEndTime() { return this.lastEndTime; }
    public static String toString(Collection<MessageCounts> countsList) {
      StringWriter  sw = new StringWriter();
      PrintWriter   pw = new PrintWriter(sw);
      pw.println();
      for (MessageCounts counts: countsList) {
        pw.println("     " + counts);
      }
      pw.println();
      pw.flush();
      return sw.toString();
    }

    public String toString() {
      synchronized (this) {
        return "MESSAGE (" + this.getMessageId()
            + "): begin=[ " + this.getBeginCount() + " / "
            + this.getFirstBeginTime() + " / " + this.getLastBeginTime()
            + " ], success=[ " + this.getSuccessCount()
            + " ], failed=[ " + this.getFailureCount()
            + " ], lastEndTime=[ " + this.getLastEndTime() + " ]";
      }
    }
  }

  public static class TestService implements ListenerService {
    private long minProcessingTime = 5000L;
    private long maxProcessingTime = 5000L;
    private List<Exception> failures = new LinkedList<>();
    private Map<Long, String> messagesByEntity = new LinkedHashMap<>();
    private Map<String, MessageCounts> countsByMessage = new LinkedHashMap<>();
    private double failureRate = 0.0;
    private int processingCount = 0;
    private boolean aborted = false;
    public TestService() {
      // do nothing
    }
    public TestService(long procesingTime, double failureRate) {
      this(procesingTime, procesingTime, failureRate);
    }

    public TestService(long   minProcessingTime,
                       long   maxProcessingTime,
                       double failureRate)
    {
      this.minProcessingTime  = minProcessingTime;
      this.maxProcessingTime  = maxProcessingTime;
      this.failureRate        = failureRate;
    }

    private synchronized void logFailure(Exception e) {
      e.printStackTrace();
      this.failures.add(e);
    }

    public synchronized List<Exception> getFailures() {
      return new ArrayList<>(this.failures);
    }

    public synchronized Map<Integer, MessageCounts> getMessageCounts() {
      Map<Integer, MessageCounts> result = new LinkedHashMap<>();
      this.countsByMessage.values().forEach((counts) -> {
        MessageCounts clone = (MessageCounts) counts.clone();
        result.put(clone.getMessageId(), clone);
      });
      return result;
    }

    public synchronized int getSuccessCount() {
      int successCount = 0;
      for (MessageCounts counts: this.countsByMessage.values()) {
        successCount += (counts.getSuccessCount() > 0) ? 1 : 0;
      }
      return successCount;
    }

    public synchronized boolean isProcessing() {
      return (this.processingCount > 0);
    }

    public synchronized void awaitSuccess(TestMessageConsumer consumer,
                                          int                 minSuccessCount)
    {
      int successCount = this.getSuccessCount();
      boolean processing = this.isProcessing();
      while ((successCount < minSuccessCount || processing) && !this.aborted) {
        try {
          this.wait(this.maxProcessingTime);

        } catch (InterruptedException ignore) {
          // ignore
        }
        successCount = this.getSuccessCount();
        processing = this.isProcessing();
      }
    }

    private synchronized MessageCounts beginProcessing(JsonObject jsonObject) {
      this.processingCount++;
      if (this.aborted) return null;
      String jsonText = JsonUtilities.toJsonText(jsonObject);
      JsonArray jsonArray = jsonObject.getJsonArray("AFFECTED_ENTITIES");
      for (JsonObject affected : jsonArray.getValuesAs(JsonObject.class)) {
        Long entityId = JsonUtilities.getLong(affected, "ENTITY_ID");
        if (this.messagesByEntity.containsKey(entityId)) {
          this.aborted = true;
          throw new IllegalStateException(
              "Simultaneous entity processing no entity " + entityId + ".  "
                  + "inProgress=[ " + this.messagesByEntity.get(entityId)
                  + " ], conflicting=[ " + jsonText + " ]");
        }
      }
      for (JsonObject affected : jsonArray.getValuesAs(JsonObject.class)) {
        Long entityId = JsonUtilities.getLong(affected, "ENTITY_ID");
        this.messagesByEntity.put(entityId, jsonText);
      }
      MessageCounts counts = this.countsByMessage.get(jsonText);
      if (counts == null) {
        counts = new MessageCounts(jsonText);
        this.countsByMessage.put(jsonText, counts);
      }
      counts.recordBegin();
      return counts;
    }

    private synchronized boolean isAborted() {
      return this.aborted;
    }

    private synchronized MessageCounts endProcessing(JsonObject  jsonObject,
                                                     boolean     success)
    {
      this.processingCount--;
      if (this.aborted) return null;
      String jsonText = JsonUtilities.toJsonText(jsonObject);
      JsonArray jsonArray = jsonObject.getJsonArray("AFFECTED_ENTITIES");
      for (JsonObject affected : jsonArray.getValuesAs(JsonObject.class)) {
        Long entityId = JsonUtilities.getLong(affected, "ENTITY_ID");
        String existing = this.messagesByEntity.get(entityId);
        if (existing == null) {
          this.aborted = true;
          throw new IllegalStateException(
              "Entity ID (" + entityId + ") was not marked for processing: "
                  + jsonText);
        }
        if (!existing.equals(jsonText)) {
          this.aborted = true;
          throw new IllegalStateException(
              "Entity ID (" + entityId + ") was associated with another "
                  + "message.  expected=[ " + jsonText + " ], found=[ "
                  + existing + " ]");
        }
      }
      for (JsonObject affected : jsonArray.getValuesAs(JsonObject.class)) {
        Long entityId = JsonUtilities.getLong(affected, "ENTITY_ID");
        this.messagesByEntity.remove(entityId);
      }
      MessageCounts counts = this.countsByMessage.get(jsonText);
      if (counts == null) {
        this.aborted = true;
        throw new IllegalStateException(
            "Missing message counts for message: " + jsonText);
      }
      if (success) counts.recordSuccess();
      else counts.recordFailure();
      this.notifyAll();
      return counts;
    }

    public void init(String config) {
      // do nothing
    }

    public void process(String message) throws ServiceExecutionException {
      JsonObject jsonObject = parseJsonObject(message);
      try {
        MessageCounts counts = this.beginProcessing(jsonObject);
        boolean success = true;
        try {
          long range = this.maxProcessingTime - this.minProcessingTime;
          double percentage = PRNG.nextDouble();
          long processingTime = this.minProcessingTime
              + ((long) (percentage * (double) range));
          int maxFailures = getInteger(
              jsonObject, "FAILURE_COUNT", 0);
          int failureCount = counts.getFailureCount();
          if (maxFailures > 0 && failureCount < maxFailures) {
            throw new ServiceExecutionException(
                "Simulated failure (" + failureCount + " of " + maxFailures
                    + ") for message: " + toJsonText(jsonObject));
          }
          boolean failure = PRNG.nextDouble() < this.failureRate;
          try {
            Thread.sleep(processingTime);
          } catch (InterruptedException ignore) {
            // do nothing
          }
          if (failure) {
            throw new ServiceExecutionException(
                "Simulated random failure for message: "
                    + toJsonText(jsonObject));
          }

        } catch (ServiceExecutionException e) {
          success = false;
          throw e;

        } catch (Exception e) {
          this.logFailure(e);
          success = false;

        } finally {
          this.endProcessing(jsonObject, success);
        }

      } catch (ServiceExecutionException e) {
        // rethrow the simulated failure
        throw e;

      } catch (Exception e) {
        this.logFailure(e);
        if (!this.isAborted()) {
          throw new ServiceExecutionException(e);
        }
      }
    }

    public void destroy() {
      // do nothing
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 4, 8 })
  public void basicTest(int concurrency) {
    List<Message> messages = new LinkedList<>();
    messages.add(new Message(1, buildInfoMessage(1,
                                                 "CUSTOMERS",
                                                 "001",
                                                 1, 2, 3)));
    messages.add(new Message(2, buildInfoMessage(2,
                                                 "CUSTOMERS",
                                                 "002",
                                                 1, 4)));
    messages.add(new Message(3, buildInfoMessage(3,
                                                 "CUSTOMERS",
                                                 "003",
                                                 2, 5)));
    messages.add(new Message(4, buildInfoMessage(4,
                                                 "CUSTOMERS",
                                                 "004",
                                                 4, 5)));
    messages.add(new Message(5, buildInfoMessage(5,
                                                 "CUSTOMERS",
                                                 "005",
                                                 6, 7)));

    this.performTest(messages,
                     messages.size(),
                     concurrency,
                     null,
                     null,
                     null,
                     null,
                     null,
                     0.0,
                     Map.of(4, Set.of(1),
                            5, Set.of(1),
                            2, Set.of(4, 5),
                            3, Set.of(4, 5)));
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 4, 8 })
  public void errantTest(int concurrency) {
    List<Message> messages = new LinkedList<>();
    messages.add(new Message(1, buildInfoMessage(1,
                                                 "CUSTOMERS",
                                                 "001",
                                                 1, 2, 3)));
    messages.add(new Message(2, buildInfoMessage(2,
                                                 1,
                                                 null,
                                                 "CUSTOMERS",
                                                 "002",
                                                 1, 4)));
    messages.add(new Message(3, buildInfoMessage(3,
                                                 "CUSTOMERS",
                                                 "003",
                                                 2, 5)));
    messages.add(new Message(4, buildInfoMessage(4,
                                                 1,
                                                 null,
                                                 "CUSTOMERS",
                                                 "004",
                                                 4, 5)));
    messages.add(new Message(5, buildInfoMessage(5,
                                                 "CUSTOMERS",
                                                 "005",
                                                 6, 7)));

    this.performTest(messages,
                     messages.size(),
                     concurrency,
                     null,
                     null,
                     null,
                     null,
                     null,
                     0.0,
                     Map.of(3, Set.of(1),
                            5, Set.of(1),
                            2, Set.of(1, 3, 5),
                            4, Set.of(1, 3, 5)));
  }

  @ParameterizedTest
  @ValueSource(ints = { 16, 32 })
  public void loadTest(int concurrency) {
    List<Message> batches = new LinkedList<>();
    int messageCount = buildInfoBatches(
        batches,
        30000,
        List.of("CUSTOMERS", "EMPLOYEES", "VENDORS"),
        1,
        10,
        1000,
        3000,
        4,
        0.005);

        System.err.println();
    System.err.println("=====================================================");
    System.err.println("Testing " + batches.size() + " batches comprising "
                       + messageCount + " messages with concurrency of "
                       + concurrency + ".");

    long start = System.nanoTime() / 1000000L;
    this.performTest(batches,
                     messageCount,
                     concurrency,
                     30,
                     50L,
                     5000L,
                     2L,
                     5L,
                     0.0,
                     null);
    long duration = (System.nanoTime() / 1000000L) - start;
    System.err.println("TOTAL TIME: " + (duration) + " ms");
  }

  protected void performTest(List<Message>              messages,
                             int                        messageCount,
                             Integer                    concurrency,
                             Integer                    dequeueCount,
                             Long                       dequeueSleep,
                             Long                       visibilityTimeout,
                             Long                       minProcessingTime,
                             Long                       maxProcessingTime,
                             Double                     failureRate,
                             Map<Integer, Set<Integer>> orderAfterMap)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    if (concurrency != null) {
      sb.append(prefix);
      sb.append("concurrency=[ " + concurrency + " ]");
      prefix = ", ";
    }
    if (dequeueCount == null) {
      dequeueCount = 2;
    } else {
      sb.append(prefix);
      sb.append("dequeueCount=[ " + dequeueCount + " ]");
      prefix = ", ";
    }
    if (dequeueSleep == null) {
      dequeueSleep = 25L;
    } else {
      sb.append(prefix);
      sb.append("dequeueSleep=[ " + dequeueSleep + " ]");
      prefix = ", ";
    }
    if (visibilityTimeout == null) {
      visibilityTimeout = 12500L;
    } else {
      sb.append(prefix);
      sb.append("visibilityTimeout=[ " + visibilityTimeout + " ]");
      prefix = ", ";
    }
    if (minProcessingTime == null) {
      minProcessingTime = 125L;
    } else {
      sb.append(prefix);
      sb.append("minProcessingTime=[ " + minProcessingTime + " ]");
      prefix = ", ";
    }
    if (maxProcessingTime == null) {
      maxProcessingTime = minProcessingTime;
    } else {
      sb.append(prefix);
      sb.append("maxProcessingTime=[ " + maxProcessingTime + " ]");
      prefix = ", ";
    }
    if (failureRate == null) {
      failureRate = 0.0;
    } else {
      sb.append(prefix);
      sb.append("failureRate=[ " + failureRate + " ]");
      prefix = ", ";
    }
    String testInfo = sb.toString();

    TestMessageConsumer consumer = new TestMessageConsumer(
        dequeueCount, dequeueSleep, visibilityTimeout, messages);

    TestService service = new TestService(
        minProcessingTime, maxProcessingTime, failureRate);

    JsonObjectBuilder job = Json.createObjectBuilder();
    if (concurrency != null) {
      job.add(CONCURRENCY_KEY, concurrency);
    }
    String consumerConfig = toJsonText(job);

    try {
      service.init("{}");
      consumer.init(consumerConfig);
      consumer.consume(service);

    } catch (MessageConsumerException exception) {
      fail(exception);
    }

    // wait success
    service.awaitSuccess(consumer, messageCount);
    //try {
    //  Thread.sleep(2000L);
    //} catch (InterruptedException ignore) {
    //  // do nothing
    //}
    consumer.destroy();
    //Map<Statistic, Number> stats = this.printStatistics(consumer, service);
    Map<Statistic, Number> stats = consumer.getStatistics();

    Number messageRetryCount  = stats.get(Statistic.messageRetryCount);
    Number processRetryCount  = stats.get(Statistic.serviceProcessRetryCount);
    Number statsFailureCount  = stats.get(Statistic.serviceProcessFailureCount);

    if (failureRate == 0.0) {
      assertEquals(consumer.getExpectedFailureCount(), statsFailureCount,
                   "Wrong number of info message failures");
      assertEquals(consumer.getExpectedMessageRetryCount(),
                   messageRetryCount,
                   "Wrong number of message (batch) retries");
      assertEquals(consumer.getExpectedInfoMessageRetryCount(),
                   processRetryCount,
                   "Wrong number of info message retries");
    }

    // get the exceptions
    int failureCount = service.getFailures().size();
    if (failureCount > 0) {
      for (Exception e: service.getFailures()) {
        System.err.println();
        System.err.println("=================================================");
        e.printStackTrace();
      }
      fail("Failed with " + failureCount + " exceptions.  " + testInfo
               + ", failures=[ " + service.getFailures() + " ]");
    }

    // get the counts
    Map<Integer, MessageCounts> countsMap = service.getMessageCounts();
    List<MessageCounts> countsList = new ArrayList<>(countsMap.values());
    Collections.sort(countsList);

    // destroy the service
    service.destroy();

    // check the message counts
    for (Message message: messages) {
      String messageBody = message.getBody();
      JsonObject jsonObject = null;
      try {
        jsonObject = parseJsonObject(messageBody);
      } catch (Exception e) {
        // bad JSON -- skip this one
        continue;
      }
      Integer messageId = getInteger(jsonObject, "MESSAGE_ID");
      if (messageId == null) continue;
      MessageCounts counts = countsMap.get(messageId);

      if (counts == null) {
        fail("Failed to find statistics for message: " + messageBody);
      }
      assertTrue((counts.getSuccessCount() > 0),
      "Message never succeeded: " + counts + " / " + messageBody);

      int maxFailures = getInteger(jsonObject, "FAILURE_COUNT", -1);
      if ((maxFailures < 0 && failureRate == 0) || (maxFailures == 0)) {
        assertEquals(0, counts.getFailureCount(),
                     "Received a failure for a message where none was "
                     + "expected: " + counts + " / " + messageBody);
      } else if (maxFailures > 0) {
        assertEquals(maxFailures, counts.getFailureCount(),
                     "Received an unexpected number of failures for "
                         + "a message: " + counts + " / " + messageBody);
      }
    }

    if (orderAfterMap != null) {
      orderAfterMap.forEach((messageId, afterSet) -> {
        MessageCounts msgCounts = countsMap.get(messageId);

        if (msgCounts == null) {
          fail("Bad test data.  Unrecognized message ID (" + messageId
                   + ") in ordering map: " + orderAfterMap + " / "
                   + countsMap);
        }
        afterSet.forEach(afterMessageId -> {
          MessageCounts afterCounts = countsMap.get(afterMessageId);
          if (afterCounts == null) {
            fail("Bad test data.  Unrecognized message ID (" + afterMessageId
                     + ") in ordering map: " + orderAfterMap + " / "
                     + countsMap);
          }
          long msgBegin = msgCounts.getLastBeginTime();
          long afterBegin = afterCounts.getLastBeginTime();
          assertTrue(msgBegin > afterBegin,
                     "Message " + messageId + " was unexpectedly "
                     + "processed before message " + afterMessageId + ": "
                     + msgBegin + " <= " + afterBegin + " / "
                         + MessageCounts.toString(countsList));
        });
      });
    }
  }

  private Map<Statistic, Number> printStatistics(TestMessageConsumer  consumer,
                                                 TestService          service)
  {
    System.err.println("COMPLETED: " + service.getSuccessCount());
    Map<Statistic, Number> stats = consumer.getStatistics();
    System.err.println("STATISTICS:");
    System.err.println(
        "  dequeueCount: " + consumer.getDequeueCount() + " messages");
    System.err.println(
        "  dequeueSleep: " + consumer.getDequeueSleep() + " ms");
    System.err.println(
        "  visibilityTimeout: " + consumer.getVisibilityTimeout() + " ms");
    System.err.println(
        "  expectedServiceProcessFailureCount: "
            + consumer.getExpectedFailureCount() + " info messages");
    System.err.println(
        "  expectedMessageRetryCount: "
            + consumer.getExpectedMessageRetryCount() + " messages");
    System.err.println(
        "  expectedInfoMessageRetryCount: "
            + consumer.getExpectedInfoMessageRetryCount() + " info messages");

    stats.forEach((key, value) -> {
      String units = key.getUnits();
      System.out.println("  " + key + ": " + value
                             + ((units != null) ? " " + units : ""));
    });
    return stats;
  }
}
