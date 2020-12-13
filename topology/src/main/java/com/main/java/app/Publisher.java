package com.main.java.app;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import com.main.java.utils.StormUtilities;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import java.util.*;

import static com.main.java.utils.Priority.*;
import static com.main.java.utils.Constants.*;

public class Publisher {

    private static boolean isLocal = false;
    private static final String ENV = "DATA_TOPOLOGY_ENV";
    private static final String AS_DB_TYPE = "aerospike";
    private static final String SPOUT_PARALLELISM_CONFIG_KEY = "spoutParallelism";
    private static final String PROD_ENV = "prod";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        String inferenceASBuckets = "list of aerospike nodes";
        // Initialize environment: prod or preprod
        String env = StringUtils.defaultIfEmpty(System.getenv(ENV), PROD_ENV);

        // Profile Demographics User Cohort
        IRichSpout userCohortDataSpout = StormUtilities.getKafkaSpoutInstance(EMPLOYEEPROFILE.toString());
        builder.setSpout(EMPLOYEEPROFILE.toString()+ "Queue", userCohortDataSpout,
                (int)ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
        builder.setBolt(EMPLOYEEPROFILE.toString() + "DataPublisher", new DatabasePublisherBolt(AS_DB_TYPE,
                        inferenceASBuckets, DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PROD),
                (int)ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "inferenceBoltParallelism"))
                .shuffleGrouping(EMPLOYEEPROFILE.toString() + "Queue");

        StormTopology topology = builder.createTopology();


        Config conf = new Config();
        conf.setDebug(ConfigClientUtils.fetchConfigWithDefaultFallback(TOPOLOGY_CONFIG_BUCKET, "isDebug", true));
        conf.setMessageTimeoutSecs((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "messageTimeout"));
        conf.setNumWorkers((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "numWorkers"));
        conf.setNumAckers((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "numAckers"));
        conf.setMaxSpoutPending((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "maxSpoutPending"));
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 10);
        StormSubmitter.submitTopologyWithProgressBar(ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "topologyName") + "-" + env , conf, topology);
    }
}