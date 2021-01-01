package com.main.java.utils;

public enum MetricRegistryName {

    NAME("name");

    private String value;

    MetricRegistryName(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
