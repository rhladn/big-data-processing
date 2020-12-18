package com.main.java.utils;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.main.java.utils.ExitCode;
import static com.main.java.utils.Constants.DELIMITER;

/**
 * Bolt receives {@link Tuple}{@code (type, key, value)}. Based on {@code type}, it selects the
 * appropriate params to write the {@code key-value} pair to the database.
 */
@Slf4j
public class DataPublisherBolt extends BaseRichBolt {

    private OutputCollector collector;
    /**
     * Holds params corresponding to various types of data.
     */
    private Map<String, IDbParams> semanticTypeToDbParams;
    private List<IDatabase<String>> databases;
    private MetricRegistry metricRegistry;
    private JmxReporter reporter;
    private String dbType;
    private List<String> configBuckets;
    private String dataTypeToDBParamBucket;
    private static final String AEROSPIKE_DB_TYPE = "aerospike";

    public DataPublisherBolt(String dbType, List<String> configBuckets, String dataTypeToDBParamBucket) {
        this.dbType = dbType;
        this.configBuckets = configBuckets;
        this.dataTypeToDBParamBucket = dataTypeToDBParamBucket;
    }

    /**
     * Returns implementation based on the type of database required.
     */
    public static <T> IDatabase<T> getDatabaseImpl(String type, String configBucket) {
        switch (type.toLowerCase()) {
            case AEROSPIKE_DB_TYPE:
                return AerospikeDatabase.getInstance(configBucket);
            default:
                throw new RuntimeException("unsupported database type");
        }
    }

    /**
     * Initializes the params for the various types of data according to
     * configs.
     */
    private void init() {
        initSemanticTypeToDbParams();
        FileClient.addListener(dataTypeToDBParamBucket, new BaseBucketUpdateListener(){
            @Override
            public void updated(Bucket oldBucket, Bucket newBucket) {
                initSemanticTypeToDbParams();
            }
        });
        databases = new ArrayList<>();
        for (String configBucket: configBuckets){
            IDatabase<String> database = getDatabaseImpl(dbType, configBucket);
            databases.add(database);
        }
        metricRegistry = new MetricRegistry();
        reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
    }

    @SuppressWarnings("unchecked")
    private void initSemanticTypeToDbParams() {
        semanticTypeToDbParams = FileClient.fetchDbParamsConfigFromGivenBucket(dataTypeToDBParamBucket);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        init();
    }

    /**
     * Extracts {@code type, key, value} from {@link Tuple} and writes data to appropriate database
     * based on type.
     */
    @Override
    public void execute(Tuple tuple) {
        String[] fields = tuple.getString(4).split(DELIMITER);
        String type = fields[0];
        String key = fields[1];
        String value = fields[2];
        IDbParams params = semanticTypeToDbParams.get(type);

        if (params != null) {
            Boolean success = true;
            for(IDatabase<String> database: databases) {
                ExitCode exitCode = database.put(key, value, params);
                if (exitCode.equals(ExitCode.FAILURE)) {
                    success = false;
                }
            }
            if (success) {
                collector.ack(tuple);
                metricRegistry.meter("storm.ack").mark();
            } else {
                collector.fail(tuple);
                metricRegistry.meter("storm.fail").mark();
            }
        } else {
            log.error("No valid Db params found for " + type);
            metricRegistry.meter("storm.invalidparams").mark();
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * Calls for each database.
     */
    @Override
    public void cleanup() {
        for(IDatabase<String> database: databases) {
            try {
                database.close();
            } catch (IOException e) {
                log.error("Unable to close connection to database", e);
            }
        }
        reporter.stop();
    }
}

