package com.main.java.utils;

import com.aerospike.client.*;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.*;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.io.IOException;
import java.util.*;

/**
 * Provides an Aerospike implementation to by using {@link AerospikeClient} to
 * create connections and perform key-value operations. <br/>
 * <b>Limitation</b>: Cannot perform operations on multiple sets or bins at once.
 */
@Slf4j
public class AerospikeClient<T> implements IDatabase<T> {

    private AerospikeClient aerospikeClient;
    private AsyncClient asyncClient;
    private MetricRegistry metricRegistry;
    private JmxReporter reporter;
    private Meter invalidParamsMeter;

    private static Map<String, AerospikeDatabase> instances = new HashMap<>();

    public static <T> AerospikeDatabase<T> getInstance(String configBucket) {
        if (!instances.containsKey(configBucket)) {
            synchronized (AerospikeDatabase.class) {
                if (!instances.containsKey(configBucket)) {
                    instances.put(configBucket, new AerospikeDatabase<>(configBucket));
                }
            }
        }
        return instances.get(configBucket);
    }

    private AerospikeDatabase(final String configBucket) {
        init(configBucket);
        // Register listener to reinitialize object in case of config bucket update.
        ExitCode exitCode = ConfigClientUtils.addListener(configBucket, new BaseBucketUpdateListener() {
            @Override
            public void updated(Bucket oldBucket, Bucket newBucket) {
                init(configBucket);
            }
        });
        if (!exitCode.equals(ExitCode.SUCCESS)) {
            log.error("Unable to register listener");
        }
        metricRegistry = new MetricRegistry();
        reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
        invalidParamsMeter = metricRegistry.meter("aerospike.invalidparams");
    }

    /**
     * Initializes object.
     */
    private void init(String configBucket) {
        List<String> hosts = ConfigClientUtils.fetchConfigCompulsary(configBucket, "seedHosts");
        Host[] hosts1 = new Host[hosts.size()];
        for (int i=0; i<hosts.size(); i++) {
            hosts1[i] = new Host(hosts.get(i).split(":")[0], Integer.parseInt(hosts.get(i).split(":")[1]));
        }
        if (aerospikeClient != null) {
            aerospikeClient.close();
        }
        if (asyncClient != null) {
            asyncClient.close();
        }
        this.aerospikeClient = new AerospikeClient(initClientPolicy(configBucket), hosts1);
        this.asyncClient = new AsyncClient(initAsyncClientPolicy(configBucket), hosts1);
    }

    /**
     * Initializes Aerospike (Read){@link Policy}.
     */
    private Policy initReadPolicy(String configBucket) {
        Policy readPolicy = new Policy();
        readPolicy.socketTimeout = FileClient(configBucket, "readTimeout", 1000);
        readPolicy.totalTimeout = FileClient(configBucket, "readTimeout", 1000);
        readPolicy.maxRetries = FileClient(configBucket, "maxReadRetries", 3);
        readPolicy.sleepBetweenRetries = 2;
        return readPolicy;
    }

    /**
     * Initializes Aerospike {@link WritePolicy}.
     */
    private WritePolicy initWritePolicy(String configBucket) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
        writePolicy.expiration = FileClient(configBucket, "writeTtl", -1);
        writePolicy.socketTimeout = FileClient(configBucket, "writeTimeout", 1000);
        writePolicy.totalTimeout = FileClient(configBucket, "writeTimeout", 1000);
        writePolicy.maxRetries = FileClient(configBucket, "maxWriteRetries", 3);
        writePolicy.sleepBetweenRetries = 2;
        return writePolicy;
    }

    /**
     * Initializes Aerospike {@link BatchPolicy}.
     */
    private BatchPolicy initBatchPolicy(String configBucket) {
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.maxConcurrentThreads = 0;
        batchPolicy.socketTimeout = FileClient(configBucket, "readTimeout", 1000);
        batchPolicy.totalTimeout = FileClient(configBucket, "readTimeout", 1000);
        batchPolicy.maxRetries = FileClient(configBucket, "maxReadRetries", 3);
        batchPolicy.maxRetries = 2;
        return batchPolicy;
    }

    /**
     * Initializes Aerospike {@link ClientPolicy}.
     */
    private ClientPolicy initClientPolicy(String configBucket) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = FileClient(configBucket, "numClientConnections", 50);
        clientPolicy.readPolicyDefault = initReadPolicy(configBucket);
        clientPolicy.writePolicyDefault = initWritePolicy(configBucket);
        clientPolicy.batchPolicyDefault = initBatchPolicy(configBucket);
        return clientPolicy;
    }

    private AsyncClientPolicy initAsyncClientPolicy(String configBucket) {
        AsyncClientPolicy clientPolicy = new AsyncClientPolicy();
        clientPolicy.writePolicyDefault = initWritePolicy(configBucket);
        return clientPolicy;
    }

    /**
     * Creates and returns an instance of {@link Key}.
     */
    private Key getKey(String key, IDbParams params) {
        String namespace = ((AerospikeParams)params).getNamespace();
        String set = ((AerospikeParams)params).getSet();
        return new Key(namespace, set, key);
    }

    private String getBin(IDbParams params) {
        return ((AerospikeParams)params).getBin();
    }

    private int getTtl(IDbParams params) {
        return ((AerospikeParams)params).getTtl();
    }

    private String getLogMessage(IDbParams params) {
        return String.format("%s.%s.%s", ((AerospikeParams)params).getNamespace(),
                ((AerospikeParams)params).getSet(), ((AerospikeParams)params).getBin());
    }

    private String getLogMessage(List<IDbParams> params) {
        String logMessage ="";
        for(IDbParams dbParam: params)
            logMessage += String.format("%s.%s.%s\t", ((AerospikeParams)dbParam).getNamespace(),
                    ((AerospikeParams)dbParam).getSet(), ((AerospikeParams)dbParam).getBin());
        return logMessage;
    }

    /**
     * Uses {@link AerospikeClient#get(Policy, Key)}.
     */
    @Override
    public T get(String key, IDbParams params) {
        if (params == null) {
            invalidParamsMeter.mark();
            return null;
        }
        Key key1 = getKey(key, params);
        Record record = null;
        Timer.Context context = metricRegistry.timer("aerospike.get").time();
        try {
            record = aerospikeClient.get(null, key1);
        } catch (AerospikeException ae) {
            log.error("Failure reading key " + key + " from " + getLogMessage(params), ae);
            metricRegistry.meter("aerospike.get.fail").mark();
        } finally {
            context.stop();
        }
        return (record != null) ? (T) record.getValue(getBin(params)) : null;
    }

    /**
     * Uses {@link AerospikeClient#put(WritePolicy, Key, Bin...)}.
     */
    @Override
    public ExitCode put(String key, T value, IDbParams params) {
        if (params == null) {
            invalidParamsMeter.mark();
            return ExitCode.INVALID;
        }
        Key key1 = getKey(key, params);
        Bin bin1 = new Bin(getBin(params), value);
        Timer.Context context = metricRegistry.timer("aerospike.put").time();
        try {
            if (getTtl(params) == 0) {
                aerospikeClient.put(null, key1, bin1);
            } else {
                WritePolicy writePolicy = new WritePolicy(aerospikeClient.writePolicyDefault);
                writePolicy.expiration = getTtl(params);
                aerospikeClient.put(writePolicy, key1, bin1);
            }
        } catch (AerospikeException ae) {
            log.error("Failure writing key " + key + " to " + getLogMessage(params), ae);
            metricRegistry.meter("aerospike.put.fail").mark();
            return ExitCode.FAILURE;
        } finally {
            context.stop();
        }
        return ExitCode.SUCCESS;
    }

    /**
     * Uses {@link AerospikeClient#get(BatchPolicy, Key[])}.
     */
    @Override
    public Map<String, T> mget(List<String> keys, IDbParams params) {
        if (CollectionUtils.isEmpty(keys)) {
            return Collections.emptyMap();
        }
        if (params == null) {
            invalidParamsMeter.mark();
            return Collections.emptyMap();
        }
        Key[] keys1 = new Key[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            keys1[i] = getKey(keys.get(i), params);
        }
        Record[] records;
        Timer.Context context = metricRegistry.timer("aerospike.mget").time();
        try {
            records = aerospikeClient.get(null, keys1);
        } catch (AerospikeException ae) {
            log.error("Failure reading keys " + keys + " from " + getLogMessage(params), ae.getMessage());
            metricRegistry.meter("aerospike.mget.fail").mark();
            return Collections.emptyMap();
        } finally {
            context.stop();
        }
        Map<String, T> result = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), (records[i] != null) ? (T) records[i].getValue(getBin(params)) : null);
        }
        return result;
    }

    /**
     * Uses {@link AerospikeClient#get(BatchPolicy, Key[])}.
     */
    @Override
    public Map<KeyParam, T> mget(List<KeyParam> keyParams) {
        if (CollectionUtils.isEmpty(keyParams)) {
            return Collections.emptyMap();
        }
        List<IDbParams> dbParams = new ArrayList<>();
        Key[] allkeys = new Key[keyParams.size()];
        for (int i=0; i<keyParams.size();i++) {
            KeyParam keyParam = keyParams.get(i);
            allkeys[i] = getKey(keyParam.getKey(), keyParam.getParams());
            dbParams.add(keyParam.getParams());
        }

        Record[] records;
        Timer.Context context = metricRegistry.timer("aerospike.mgetKeyParams").time();
        try {
            records = aerospikeClient.get(null, allkeys);
        } catch (AerospikeException ae) {
            log.error("Failure reading keys " + allkeys + " from " + getLogMessage(dbParams), ae.getMessage());
            metricRegistry.meter("aerospike.mgetKeyParams.fail").mark();
            return Collections.emptyMap();
        } finally {
            context.stop();
        }
        Map<KeyParam, T> result = new HashMap<>();
        for (int i=0; i<keyParams.size();i++) {
            result.put(keyParams.get(i), (records[i] != null) ?
                    (T) records[i].getValue(getBin(keyParams.get(i).getParams())) : null);
        }
        return result;
    }

    /**
     * Uses {@link AsyncClient#put(WritePolicy, WriteListener, Key, Bin...)}.
     */
    @Override
    public Map<String, ExitCode> mput(Map<String, T> entries, final IDbParams params) {
        if (MapUtils.isEmpty(entries)) {
            return Collections.emptyMap();
        }
        if (params == null) {
            invalidParamsMeter.mark();
            Map<String, ExitCode> result = new HashMap<>();
            for (Map.Entry<String, T> entry: entries.entrySet()) {
                result.put(entry.getKey(), ExitCode.INVALID);
            }
            return result;
        }
        Map<String, ExitCode> result = new HashMap<>();
        for (Map.Entry<String, T> entry: entries.entrySet()) {
            Bin bin = new Bin(getBin(params), entry.getValue());
            ExitCode exitCode;
            Timer.Context context = metricRegistry.timer("aerospike.async.put").time();
            try {
                if (getTtl(params) == 0) {
                    asyncClient.put(null, getKey(entry.getKey(), params), bin);
                } else {
                    WritePolicy writePolicy = new WritePolicy(aerospikeClient.writePolicyDefault);
                    writePolicy.expiration = getTtl(params);
                    asyncClient.put(writePolicy, getKey(entry.getKey(), params), bin);
                }
                exitCode = ExitCode.SUCCESS;
            } catch(AerospikeException ae) {
                log.error("Failure writing key " + entry.getKey() + " to " + getLogMessage(params), ae);
                metricRegistry.meter("aerospike.async.put.fail").mark();
                exitCode = ExitCode.FAILURE;
            } finally {
                context.stop();
            }
            result.put(entry.getKey(), exitCode);
        }
        return result;
    }

    /**
     * Uses {@link AerospikeClient#exists(Policy, Key)}.
     */
    @Override
    public Boolean exists(String key, IDbParams params) {
        if (params == null) {
            invalidParamsMeter.mark();
            return false;
        }
        Key key1 = getKey(key, params);
        Timer.Context context = metricRegistry.timer("aerospike.exists").time();
        try {
            return aerospikeClient.exists(null, key1);
        } catch (AerospikeException ae) {
            log.error("Failure checking exists for key " + key + " from " + getLogMessage(params), ae);
            metricRegistry.meter("aerospike.exists.fail").mark();
            return false;
        } finally {
            context.stop();
        }
    }

    /**
     * Uses {@link AerospikeClient#exists(BatchPolicy, Key[])}.
     */
    @Override
    public Map<String, Boolean> mexists(List<String> keys, IDbParams params) {
        if (params == null) {
            invalidParamsMeter.mark();
            Map<String, Boolean> result = new HashMap<>();
            for (String key: keys) {
                result.put(key, false);
            }
            return result;
        }
        if (CollectionUtils.isEmpty(keys)) {
            return Collections.emptyMap();
        }
        Map<String, Boolean> result = new HashMap<>();
        Key[] keys1 = new Key[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            keys1[i] = getKey(keys.get(i), params);
        }
        boolean[] existStatuses;
        Timer.Context context = metricRegistry.timer("aerospike.mexists").time();
        try {
            existStatuses = aerospikeClient.exists(null, keys1);
        } catch (AerospikeException ae) {
            log.error("Failure checking exists for keys " + keys + " from " + getLogMessage(params), ae);
            metricRegistry.meter("aerospike.mexists.fail").mark();
            for (String key: keys) {
                result.put(key, false);
            }
            return result;
        } finally {
            context.stop();
        }
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), existStatuses[i]);
        }
        return result;
    }

    /**
     * Uses {@link AerospikeClient#delete(WritePolicy, Key)}.
     */
    @Override
    public ExitCode delete(String key, IDbParams params) {
        if (params == null) {
            invalidParamsMeter.mark();
            return ExitCode.INVALID;
        }
        Key key1 = getKey(key, params);
        Timer.Context context = metricRegistry.timer("aerospike.delete").time();
        try {
            aerospikeClient.delete(null, key1);
        } catch (AerospikeException ae) {
            log.error("Failure deleting key " + key + " from " + getLogMessage(params), ae);
            metricRegistry.meter("aerospike.delete.fail").mark();
            return ExitCode.FAILURE;
        } finally {
            context.stop();
        }
        return ExitCode.SUCCESS;
    }

    /**
     * Uses {@link AsyncClient#delete(WritePolicy, DeleteListener, Key)}.
     */
    @Override
    public Map<String, ExitCode> mdelete(List<String> keys, final IDbParams params) {
        if (CollectionUtils.isEmpty(keys)) {
            return Collections.emptyMap();
        }
        if (params == null) {
            invalidParamsMeter.mark();
            Map<String, ExitCode> result = new HashMap<>();
            for (String key: keys) {
                result.put(key, ExitCode.INVALID);
            }
            return result;
        }
        final Map<String, ExitCode> result = new HashMap<>();
        for (String key: keys) {
            ExitCode exitCode;
            Timer.Context context = metricRegistry.timer("aerospike.async.delete").time();
            try {
                asyncClient.delete(null, getKey(key, params));
                exitCode = ExitCode.SUCCESS;
            } catch (AerospikeException ae) {
                metricRegistry.meter("aerospike.async.delete.fail").mark();
                log.error("Failure deleting key " + key + " from " + getLogMessage(params), ae);
                exitCode = ExitCode.FAILURE;
            } finally {
                context.stop();
            }
            result.put(key, exitCode);
        }
        return result;
    }

    @Override
    /**
     * Calls {@link AerospikeClient#close()} to terminate connection
     */
    public void close() throws IOException {
        aerospikeClient.close();
        reporter.close();
    }

}
