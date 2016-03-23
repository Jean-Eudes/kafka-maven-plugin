package io.jean_eudes.maven;

import static org.apache.maven.plugins.annotations.LifecyclePhase.POST_INTEGRATION_TEST;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import scala.collection.*;
import scala.collection.Iterable;

@Mojo(name = "deleteAllTopics",
      defaultPhase = POST_INTEGRATION_TEST,
      threadSafe = true)
public class KafkaDeleteAllTopicsExecuteMojo extends AbstractMojo {

    @Parameter(property = "zookeeper.host", defaultValue = "localhost")
    private String zookeeperHost;

    @Parameter(property = "zookeeper.port", defaultValue = "2181")
    private int zookeeperPort;

    public void execute() throws MojoExecutionException, MojoFailureException {
        ZkUtils zkUtils = ZkUtils.apply(String.format("%s:%d", zookeeperHost, zookeeperPort), 10000, 10000, false);

        Iterator<String> stringPropertiesMap = AdminUtils.fetchAllTopicConfigs(zkUtils).keysIterator();

        while (stringPropertiesMap.hasNext()) {
            String topic = stringPropertiesMap.next();

            AdminUtils.deleteTopic(zkUtils, topic);

            long start = System.currentTimeMillis();
            long timeout = 10000;

            while (AdminUtils.topicExists(zkUtils, topic) && (System.currentTimeMillis() - start) < timeout) {

                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    getLog().warn(e);
                }
            }

            if ((System.currentTimeMillis() - start) > timeout) {
                getLog().warn(String.format("Topic '%s' deletion failed", topic));
            }

        }

    }

}
