package com.main.java.utils;

public enum Priority {

    EMPLOYEEPROFILE("UC");

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
