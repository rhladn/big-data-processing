package com.main.java.utils;

public enum Priority {
    @Deprecated
    HIGH("high"),
    @Deprecated
    MEDIUM("medium"),
    @Deprecated
    LOW("low"),
    @Deprecated
    TEST("test"),
    @Deprecated
    PRODUCTINFO("productinfo"),

    AUXILIARY("auxiliary"),
    DEBUG("debug"),
    INFERENCE("inference"),
    P2PRECO("p2preco"),
    P2PSEEDING("p2pseeding"),
    USERCOHORT("UC");

    String value;

    Priority(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static Priority fromString(String priority) {
        return Priority.valueOf(priority.toUpperCase());
    }
}
