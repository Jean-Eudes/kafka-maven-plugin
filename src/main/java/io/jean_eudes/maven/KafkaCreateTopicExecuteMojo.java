package io.jean_eudes.maven;

import java.util.List;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import static org.apache.maven.plugins.annotations.LifecyclePhase.PRE_INTEGRATION_TEST;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "createTopic",
        defaultPhase = PRE_INTEGRATION_TEST,
        threadSafe = true)
public class KafkaCreateTopicExecuteMojo extends AbstractMojo {

    @Parameter(property = "topics", required = true)
    private List<String> topics;

    @Parameter(property = "zookeeperHost", defaultValue = "localhost")
    private String zookeeperHost;

    @Parameter(property = "zookeeperPort", defaultValue = "2181")
    private int zookeeperPort;

    @Parameter(property = "partition", defaultValue = "1")
    private int partition;

    @Parameter(property = "replicationFactor", defaultValue = "1")
    private int replicationFactor;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (topics.isEmpty()) {
            throw new MojoExecutionException("Topics must be specified!");
        }
        ZkUtils zkUtils = ZkUtils.apply(String.format("%s:%d", zookeeperHost, zookeeperPort), 10000, 10000, false);

        for (String topic : topics) {
            getLog().info("creating topic: " + topic);
            AdminUtils.createTopic(zkUtils, topic, partition, replicationFactor, new Properties());
        }
    }

}
