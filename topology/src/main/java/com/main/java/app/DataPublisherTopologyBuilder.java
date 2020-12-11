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

public class DataPublisherTopologyBuilder {

    private static boolean isLocal = false;
    private static final String ENV = "RECO_DATA_TOPOLOGY_ENV";
    private static final String MISC_TOPICS_CONFIG_KEY = "miscTopics";
    private static final String AS_DB_TYPE = "aerospike";
    private static final String BOLT_PARALLELISM_CONFIG_KEY = "boltParallelism";
    private static final String SPOUT_PARALLELISM_CONFIG_KEY = "spoutParallelism";
    private static final String AEROSPIKE_CONFIG_BUCKETS_KEY = "aerospikeBuckets";
    private static final String INFERENCE_AEROSPIKE_CONFIG_BUCKETS_KEY = "inferenceASBuckets";
    private static final String PROD_ENV = "prod";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // Initialize environment: prod or preprod
        String env = StringUtils.defaultIfEmpty(System.getenv(ENV), PROD_ENV);

        // Initialize bolts and spouts
        Map<String, IRichSpout> spouts = new HashMap<>();
        List<BoltDeclarer> boltDeclarers = new ArrayList<>();

        if (env.equalsIgnoreCase(PROD_ENV)) {
            List<String> miscTopicsList = ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, MISC_TOPICS_CONFIG_KEY);
            Set<String> miscTopics = Sets.newHashSet(miscTopicsList);
            //Spout for auxiliary data like p2Meta, store-store, pc-pc etc
            if (miscTopics.contains(AUXILIARY.toString())) {
                String auxiliaryDataTopicName = AUXILIARY.toString() + "Queue";
                IRichSpout auxiliaryDataSpout = StormUtilities.getKafkaSpoutInstance(AUXILIARY.toString());
                builder.setSpout(auxiliaryDataTopicName, auxiliaryDataSpout,
                        (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
                builder.setBolt("auxiliaryDataPublisher", new DatabasePublisherBolt(AS_DB_TYPE,
                                Collections.singletonList(AEROSPIKE_AUXILIARY_CONFIG_BUCKET), DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PROD),
                        (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, BOLT_PARALLELISM_CONFIG_KEY))
                        .shuffleGrouping(auxiliaryDataTopicName);
            }

            List<String> inferenceASBuckets = ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, INFERENCE_AEROSPIKE_CONFIG_BUCKETS_KEY);
            if (miscTopics.contains(INFERENCE.toString())) {
                //Spout for inference data like p2s, p2wpc,p2pg etc and meta p2v
                String sherlockInferenceTopicName = INFERENCE.toString() + "Queue";
                IRichSpout sherlockInferenceDataSpout = StormUtilities.getKafkaSpoutInstance(INFERENCE.toString());
                builder.setSpout(sherlockInferenceTopicName, sherlockInferenceDataSpout,
                        (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
                builder.setBolt("sherlockInferenceDataPublisher", new DatabasePublisherBolt(AS_DB_TYPE,
                                inferenceASBuckets, DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PROD),
                        (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "inferenceBoltParallelism"))
                        .shuffleGrouping(sherlockInferenceTopicName);
            }

            // Set spouts and bolts for p2p and p2p seeding.
            IRichSpout p2pBytesDataSpout = StormUtilities.getKafkaSpoutInstance(P2PRECO.toString());

            //TODO: remove the old bucket from config after migration
            List<String> configBuckets = ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, AEROSPIKE_CONFIG_BUCKETS_KEY);
            builder.setSpout(P2PRECO.toString() + "Queue", p2pBytesDataSpout,
                    (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
            builder.setBolt(P2PRECO.toString() + "DataPublisher", new ByteArrayDataPublisherBolt(AS_DB_TYPE,
                            configBuckets, DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PROD),
                    (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, BOLT_PARALLELISM_CONFIG_KEY))
                    .shuffleGrouping(P2PRECO.toString() + "Queue");

            IRichSpout p2pSeedingBytesDataSpout = StormUtilities.getKafkaSpoutInstance(P2PSEEDING.toString());
            builder.setSpout(P2PSEEDING.toString() + "Queue", p2pSeedingBytesDataSpout,
                    (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
            builder.setBolt(P2PSEEDING.toString() + "DataPublisher", new ByteArrayDataPublisherBolt(AS_DB_TYPE,
                            configBuckets, DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PROD),
                    (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, BOLT_PARALLELISM_CONFIG_KEY))
                    .shuffleGrouping(P2PSEEDING.toString() + "Queue");

            // Profile Demographics User Cohort
            IRichSpout userCohortDataSpout = StormUtilities.getKafkaSpoutInstance(USERCOHORT.toString());
            builder.setSpout(USERCOHORT.toString()+ "Queue", userCohortDataSpout,
                    (int)ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
            builder.setBolt(USERCOHORT.toString() + "DataPublisher", new DatabasePublisherBolt(AS_DB_TYPE,
                            inferenceASBuckets, DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PROD),
                    (int)ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "inferenceBoltParallelism"))
                    .shuffleGrouping(USERCOHORT.toString() + "Queue");
        } else {
            // Add debug priority topic
            spouts.put(DEBUG.toString(), StormUtilities.getKafkaSpoutInstance(DEBUG.toString()));
            // Add bolt
            boltDeclarers.add(builder.setBolt("preprodDatabasePublisher", new DatabasePublisherBolt(AS_DB_TYPE,
                            Collections.singletonList(AEROSPIKE_PREPROD_CONFIG_BUCKET), DATA_TYPE_TO_DATABASE_PARAMS_BUCKET_PREPROD),
                    (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, BOLT_PARALLELISM_CONFIG_KEY)));
        }

        for (Map.Entry<String, IRichSpout> entry : spouts.entrySet()) {
            String spoutId = entry.getKey() + "queue";
            builder.setSpout(spoutId, entry.getValue(),
                    (int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, SPOUT_PARALLELISM_CONFIG_KEY));
            for (BoltDeclarer boltDeclarer : boltDeclarers) {
                boltDeclarer.shuffleGrouping(spoutId);
            }
        }

        StormTopology topology = builder.createTopology();

        if (!isLocal) {
            Config conf = new Config();
            conf.setDebug(ConfigClientUtils.fetchConfigWithDefaultFallback(TOPOLOGY_CONFIG_BUCKET, "isDebug", true));
            conf.setMessageTimeoutSecs((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "messageTimeout"));
            conf.setNumWorkers((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "numWorkers"));
            conf.setNumAckers((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "numAckers"));
            conf.setMaxSpoutPending((int) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "maxSpoutPending"));
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 10);
            StormSubmitter.submitTopologyWithProgressBar(ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "topologyName") + "-" + env , conf, topology);
        } else {
            Config conf = new Config();
            conf.setDebug(false);
            conf.setMessageTimeoutSecs(1000);
            conf.setMaxSpoutPending(1000);
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology((String) ConfigClientUtils.fetchConfigCompulsary(TOPOLOGY_CONFIG_BUCKET, "topologyName"), conf, topology);
            Thread.sleep(1000000);
            cluster.shutdown();
        }
    }
}
