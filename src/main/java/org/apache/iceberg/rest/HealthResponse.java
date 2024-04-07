package org.apache.iceberg.rest;

import java.util.Map;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class HealthResponse implements RESTResponse {

    private Map<String, Map<String, Integer>> data;
    private Boolean databaseAvailable;

    public HealthResponse() {
    }

    public HealthResponse(Map<String, Map<String, Integer>> data, Boolean databaseAvailable) {
        this.data = data;
        this.databaseAvailable = databaseAvailable;
        validate();
    }

    public Map<String, Map<String, Integer>> data() {
        return this.data;
    }

    public Boolean databaseAvailable() {
        return this.databaseAvailable;
    }

    @Override
    public void validate() {
        Preconditions.checkArgument(this.data != null, "Invalid data: null");
    }

}
