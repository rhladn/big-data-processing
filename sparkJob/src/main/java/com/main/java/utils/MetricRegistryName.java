package com.main.java.utils;

public enum MetricRegistryName {

    PHOENIX("phoenix");

    private String value;

    MetricRegistryName(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
