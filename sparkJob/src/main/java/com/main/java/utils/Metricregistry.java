package com.main.java.utils;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import lombok.Getter;

//Provides component specific metricRegistry
public class Metricregistry {

    private static Metricregistry instance;
    @Getter
    private MetricRegistry metricRegistry;
    private JmxReporter reporter;

    private Metricregistry(String registryName) {
        metricRegistry = SharedMetricRegistries.getOrCreate(registryName);
        reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));
    }

    /**
     * This method provides component specific singleton instance for RecoMetricRegistry
     *
     * @param registryName - component name
     *
     * @return - Singleton instance for RecoMetricRegistry
     */
    public static Metricregistry getInstance(MetricRegistryName registryName) {
        if (instance == null) {
            synchronized (Metricregistry.class) {
                if (instance == null) {
                    instance = new Metricregistry(registryName.toString());
                    return instance;
                }
            }
        }
        return instance;
    }

    private class ShutDownHook implements Runnable {

        @Override
        public void run() {
            reporter.stop();
        }
    }
}