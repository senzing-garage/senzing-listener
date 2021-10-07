# senzing-listener

## Overview

The Senzing Listener is a Java framework that combines access to a queue (RabbitMQ or SQS) and G2 API.  It facilitates the creation of applications that would receive information (e.g. through streaming service) about G2 entities and perform actions against G2, based on that information.

## Setup and building

Before using the Senzing Listener to create applications you will need to build it and install in the local Maven repository.

### Dependencies

To build the Senzing Listener you will need Apache Maven (recommend version 3.6.1 or later)
as well as OpenJDK version 11.0.x (recommend version 11.0.6+10 or later).

The Senzing Listener depends on `g2.jar`.  Version 2.9.0 and later of `g2.jar` 
is available from the Central Maven Repository.  Version `2.9.x` of `g2.jar` 
supports all Senzing 2.x product versions.  However, the Senzing Listener
supports version 1.14.x or later.  If you need version 1.x then you must install
`g2.jar` version 1.x in your local Maven repository via the following steps:

1. Locate your
   [SENZING_G2_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_g2_dir)
   directory.
   The default locations are:
    1. [Linux](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-senzing-api.md#centos): `/opt/senzing/g2`
    1. Windows MSI Installer: `C:\Program Files\Senzing\`

1. Determine your `SENZING_G2_JAR_VERSION` version number:
    1. Locate your `g2BuildVersion.json` file:
        1. Linux: `${SENZING_G2_DIR}/g2BuildVersion.json`
        1. Windows: `${SENZING_G2_DIR}\data\g2BuildVersion.json`
    1. Find the value for the `"VERSION"` property in the JSON contents.
       Example:

        ```console
        {
            "PLATFORM": "Linux",
            "VERSION": "1.14.20060",
            "API_VERSION": "1.14.3",
            "BUILD_NUMBER": "2020_02_29__02_00"
        }
        ```

1. Install the `g2.jar` file in your local Maven repository, replacing the
   `${SENZING_G2_DIR}` and `${SENZING_G2_JAR_VERSION}` variables as determined above:

    1. Linux:

        ```console
        export SENZING_G2_DIR=/opt/senzing/g2
        export SENZING_G2_JAR_VERSION=1.14.3

        mvn install:install-file \
            -Dfile=${SENZING_G2_DIR}/lib/g2.jar \
            -DgroupId=com.senzing \
            -DartifactId=g2 \
            -Dversion=${SENZING_G2_JAR_VERSION} \
            -Dpackaging=jar
        ```

    1. Windows:

        ```console
        set SENZING_G2_DIR="C:\Program Files\Senzing\g2"
        set SENZING_G2_JAR_VERSION=1.14.3

        mvn install:install-file \
            -Dfile="%SENZING_G2_DIR%\lib\g2.jar" \
            -DgroupId=com.senzing \
            -DartifactId=g2 \
            -Dversion="%SENZING_G2_JAR_VERSION%" \
            -Dpackaging=jar
        ```

### Building

To build simply execute:

```console
mvn install
```

## Creating applications.

There are 3 steps needed for creating an application around the Senzing Listener framework.

1. Generate a consumer that interacts with RabbitMQ or SQS.

    This is done with a factory class, `MessageConsumerFactory`.  The Method is 

    ```console
    MessageConsumer generateMessageConsumer(ConsumerType consumerType, String config)
    ```

    The `consumerType` specifies what type of consumer is used. For RabbitMQ the value would be ConsumerType.rabbitmq; for SQS the value would be ConsumerType.sqs. Note: SQS currently supports role credentials only.

    The `config` parameter is in Json format and takes 4 values:

    1. (RabbitMQ only) mqHost which specifies the host interfact to the RabbitMQ.

    1. (RabbitMQ or SQS) mqQueue which gives the name of the queue being consumed from.

    1. (RabbitMQ only) mqUser, the name of the user for accessing the queue. This is optional, depending on RabbitMQ's security setting.

    1. (RabbitMQ only) mqPassword, the password for the user. This is optional.

    An example could be:

    {"mqHost":"localhost","mqQueue":"workQueue","mqUser":"user1","mqPassword":"pw"}

1. Create a service that does the desired operations, like process the messages received or interact with G2.

    This service is implemented from an interface `ListenerService`.  There are 2 methods required.

    1. init(String config) where 'config` is a string containing configuration information. The format of the configuration is not set or enforced but Json is the desired form
    
    1. process(String message) where `message` is the message received from the queue. The format is not set or enforced but will probably be in Jason format.

1. Pass the service generated in the second steo, to the consumer generated in the first step

This is done by calling the function `consume` method in the MessageConsumer.

## Implementation examples

### Hello World

This "Hello World" example will read messages from RabbitMQ and print "Hello World!" to the console each time it receives one.

1. Go to directory where you can create the app and make a directory:
    mkdir -p src/main/java

1. Create 2 files in this directory:
    1. src/main/java/HelloWorldService.java
        With the content:

        ```console
        import com.senzing.listener.service.ListenerService;
        import com.senzing.listener.service.exception.ServiceExecutionException;
        import com.senzing.listener.service.exception.ServiceSetupException;

        public class HelloWorldService implements ListenerService {
          @Override
          public void init(String config) throws ServiceSetupException {
            System.out.println("Application has started.  Press ^c to stop.");
          }

          @Override
          public void process(String message) throws ServiceExecutionException {
            System.out.println("Hello World!  I received this message:");
            // Echo the message, received from RabbitMQ, to the console.
            System.out.println(message);
          }
        }
        ```

    1. src/main/java/HelloWorldAPP.java

        ```console
        import com.senzing.listener.communication.ConsumerType;
        import com.senzing.listener.communication.MessageConsumer;
        import com.senzing.listener.communication.MessageConsumerFactory;
        import com.senzing.listener.service.ListenerService;
        import com.senzing.listener.service.exception.ServiceExecutionException;
        import com.senzing.listener.service.exception.ServiceSetupException;

        public class HelloWorldApp {
          public static void main(String[] args) {
            // The required configuration, mq name and the host RabbitMQ runs on.
            String config = "{\"mqQueue\":\"hwqueue\",\"mqHost\":\"localhost\"}";

            try {
              // Create the service and initialize it.
              ListenerService service = new HelloWorldService();
              // In this simple the initalization is not needed but is included for demonstration purposes.
              service.init(config);

              // Generate the queue consumer.
              MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(ConsumerType.rabbitmq, config);

              // Pass the service to the consumer, which will do the processing.
              consumer.consume(service);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
        ```

1. In your current directory create a pom file:

    ```console
    <project xmlns="http://maven.apache.org/POM/4.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
      https://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>
      <groupId>hello.world</groupId>
      <artifactId>hello-world-app</artifactId>
      <version>0.1</version>
      <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>${maven.compiler.source}</maven.compiler.target>
      </properties>
      <dependencies>
        <dependency>
          <groupId>com.senzing</groupId>
          <artifactId>senzing-listener</artifactId>
          <version>0.0.1-SNAPSHOT</version>
        </dependency>
      </dependencies>
      <build>
        <plugins>
          <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-jar-plugin</artifactId>
            <version>3.0.2</version>
            <configuration>
              <archive>
                <manifest>
                  <addClasspath>true</addClasspath>
                  <classpathPrefix>libs/</classpathPrefix>
                  <mainClass>
                    HelloWorldApp
                  </mainClass>
                </manifest>
              </archive>
            </configuration>
          </plugin>
          <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-dependency-plugin</artifactId>
            <version>3.0.2</version>
            <executions>
              <execution>
                <id>copy-dependencies</id>
                <phase>prepare-package</phase>
                <goals>
                  <goal>copy-dependencies</goal>
                </goals>
                <configuration>
                  <outputDirectory>${project.build.directory}/libs</outputDirectory>
                </configuration>
              </execution>
            </executions>
          </plugin>
        </plugins>
      </build>
    </project>
    ```

Note that anything in the `<build>` section is technically not needed but it makes running the app a lot easier.

1. Build the app:

```console
mvn install
```

### Test the Hello World app

1. Set up RabbitMQ

    1. Go to RabbitMQ [console http://localhost:15672]

    1. Log in guest/guest

    1. Click Queues tab

    1. Click "Add a new queue"

    1. Enter "hwqueue" in "Name:" box.

    1. Click "Add Queue" button.

1. Run the Hello World app

    ```console
    java -jar target/hello-world-app-0.1.jar
    ```

1. Send a message from RabbitMQ to app

    1. Go back to the RabbitMQ [console http://localhost:15672]

    1. Click on the queue name

    1. Enter text in the "Payload:" box e.g. "Testing Hello World app!"

    1. Press the "Publish message" button.

1. Verify reception by app

    You should get 

    ```console
    Hello World!  I received this message:
    Testing Hello World app!
    ```

### G2 implementation

This example adds access to G2 to the Hello World example above.

1. Modify `In your current directory create a pom file:` by adding ini file to the config (that sould be the only change)

    ```console
    import com.senzing.listener.communication.ConsumerType;
    import com.senzing.listener.communication.MessageConsumer;
    import com.senzing.listener.communication.MessageConsumerFactory;
    import com.senzing.listener.service.ListenerService;
    import com.senzing.listener.service.exception.ServiceExecutionException;
    import com.senzing.listener.service.exception.ServiceSetupException;

    public class HelloWorldApp {
      public static void main(String[] args) {
        // The required configuration, mq name and the host RabbitMQ runs on.
        // Note: the ini file path needs to be adjusted to match the Senzing G2 installation.
        String config = "{\"mqQueue\":\"hwqueue\",\"mqHost\":\"localhost\",\"iniFile\":\"/home/user/senzing/etc/G2Module.ini\"}";
    
        try {
          // Create the service and initialize it.
          ListenerService service = new HelloWorldService();
          // In this simple the initalization is not needed but is included for demonstration purposes.
          service.init(config);
    
          // Generate the queue consumer.
          MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(ConsumerType.rabbitmq, config);
    
          // Pass the service to the consumer, which will do the processing.
          consumer.consume(service);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    ```

1. Modify `src/main/java/HelloWorldService.java` by adding G2 initialization and getting the entity from G2.  The Senzing Listener has a helper class called `G2Service` which makes connecting to G2 easy.  It has a couple of methods for getting data from G2 and feel free to add more methods to it as you need.

    ```console
    import org.json.JSONArray;
    import org.json.JSONException;
    import org.json.JSONObject;
    
    import com.senzing.listener.service.g2.G2Service;
    import com.senzing.listener.service.ListenerService;
    import com.senzing.listener.service.exception.ServiceExecutionException;
    import com.senzing.listener.service.exception.ServiceSetupException;
    
    public class HelloWorldService implements ListenerService {
    
      G2Service g2Service;
    
      @Override
      public void init(String config) throws ServiceSetupException {
        // Get the ini file name from configuration.
        String g2IniFile = null;
        try { 
          JSONObject configObject = new JSONObject(config);
          g2IniFile = configObject.optString("iniFile");
        } catch (JSONException e) {
          throw new ServiceSetupException(e);
        }
        // Initalize G2.
        g2Service = new G2Service();
        g2Service.init(g2IniFile);
    
        System.out.println("Application has started.  Press ^c to stop.");
      }
    
      @Override
      public void process(String message) throws ServiceExecutionException {
        try {
          // Parse the Json string.
          JSONObject json = new JSONObject(message);
          // Get the entity IDs out of the message.
          JSONArray entities = json.getJSONArray("AFFECTED_ENTITIES");
          if (entities != null) {
            for (int i = 0; i < entities.length(); i++) {
              JSONObject entity = entities.getJSONObject(i);
              if (entity != null) {
                Long entityID = entity.getLong("ENTITY_ID");
                String entityData = g2Service.getEntity(entityID, false, false);
                System.out.println("G2 entity:");
                System.out.println(entityData);
              }
            }
          }
        } catch (JSONException e) {
          throw new ServiceExecutionException(e);
        }
    
      }
    }
    ```

1. Build it just like in the Hello World example.  The pom.xml is the same.

1. Since we have added G2 to the mix you need to etup your environment. The G2 API's rely on native libraries and the environment must be properly setup to find those libraries:

    Linux

    ```console
    export SENZING_G2_DIR=/opt/senzing/g2

    export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:${SENZING_G2_DIR}/lib/debian:$LD_LIBRARY_PATH
    ```

    Windows

    ```console
    set SENZING_G2_DIR="C:\Program Files\Senzing\g2"

    set Path=%SENZING_G2_DIR%\lib;%Path%
    ```

1. For testing follow the steps in the Hello World example, except use 

    {"DATA_SOURCE":"TEST","RECORD_ID":"RECORD3","AFFECTED_ENTITIES":[{"ENTITY_ID":1,"LENS_CODE":"DEFAULT"}]}` 

    as a payload message.  You should get the G2 entity echoed to the screen.  You might need to adjust the above message if entity id 1 doesn't exist in your system.

