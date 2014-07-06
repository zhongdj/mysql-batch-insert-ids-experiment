package com.eventbank.experiments.jdbc;

public class ValidationResult {

    private final AutoIncrementedData data;
    private final boolean consistency;

    public ValidationResult(final AutoIncrementedData data, final boolean success) {
        this.data = data;
        this.consistency = success;
    }

    // TODO Barry how to create the content of the consistency array

    public boolean isSuccess() {
        return consistency;
    }

    @Override
    public String toString() {
        return "ValidationResult [data=" + data + ", consistency=" + consistency + "]";
    }

}
