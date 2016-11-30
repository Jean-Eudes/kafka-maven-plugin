package io.jean_eudes.maven;

import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import static org.apache.maven.plugins.annotations.LifecyclePhase.PRE_INTEGRATION_TEST;

@Mojo(name = "createTopic",
      defaultPhase = PRE_INTEGRATION_TEST,
      threadSafe = true)
public class KafkaCreateTopicExecuteMojo extends AbstractMojo {

    @Parameter(property = "kafka.topic", defaultValue = "myTopic")
    private String topic;

    @Parameter(property = "zookeeper.host", defaultValue = "localhost")
    private String zookeeperHost;

    @Parameter(property = "zookeeper.port", defaultValue = "2181")
    private int zookeeperPort;

    @Parameter(property = "partition", defaultValue = "1")
    private int partition;

    @Parameter(property = "replicationFactor", defaultValue = "1")
    private int replicationFactor;

    @Parameter(property = "kafka.skip", defaultValue = "false")
    private boolean skip;

    public void execute() throws MojoExecutionException, MojoFailureException {
        if(skip) {
            getLog().info("Skips creating topic : " + topic);
            return;
        }

        getLog().info("creating topic: " + topic);

        ZkUtils zkUtils = ZkUtils.apply(String.format("%s:%d", zookeeperHost, zookeeperPort), 10000, 10000, false);
        AdminUtils.createTopic(zkUtils, topic, partition, replicationFactor, new Properties());
    }

}
