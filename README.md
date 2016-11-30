## Simple kafka topic management maven plugin

Sample configuration

```xml
<plugin>
    <groupId>io.jean_eudes.maven</groupId>
    <artifactId>kafka-maven-plugin</artifactId>
    <version>0.5</version>
    <executions>
        <execution>
            <id>remove-all-topics_before_build</id>
            <phase>pre-integration-test</phase>
            <goals>
                <goal>deleteAllTopics</goal>
            </goals>
        </execution>
        <execution>
            <id>create-magic-topic</id>
            <phase>pre-integration-test</phase>
            <goals>
                <goal>createTopic</goal>
            </goals>
            <configuration>
                <topic>magic_topic</topic>
                <partition>1</partition>
                <replicationFactor>1</replicationFactor>
            </configuration>
        </execution>
        <execution>
            <id>remove-all-topics_after_build</id>
            <phase>post-integration-test</phase>
            <goals>
                <goal>deleteAllTopics</goal>
            </goals>
        </execution>
    </executions>
    <configuration>
        <zookeeperHost>${zookeeper.host}</zookeeperHost>
        <zookeeperPort>${zookeeper.port}</zookeeperPort>
    </configuration>
</plugin>
```

