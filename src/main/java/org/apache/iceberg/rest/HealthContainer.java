package org.apache.iceberg.rest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HealthContainer {
    private ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicInteger>> data = new ConcurrentHashMap<>();

    public void add(String namespace, String key) {
        ConcurrentHashMap<String, AtomicInteger> namespaceValue = data.get(namespace);
        if (namespaceValue == null) {
            namespaceValue = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, AtomicInteger> oldValue = data.putIfAbsent(namespace, namespaceValue);
            if (oldValue != null) {
                namespaceValue = oldValue;
            }
        }
        AtomicInteger value = namespaceValue.get(key);
        if (value == null) {
            value = new AtomicInteger();
            AtomicInteger oldValue = namespaceValue.putIfAbsent(key, value);
            if (oldValue != null) {
                value = oldValue;
            }
        }
        value.incrementAndGet();
    }

    public HealthResponse buildResponse(Boolean databaseAvailable) {
        return new HealthResponse(
                data.entrySet().stream().collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                x -> x.getValue().entrySet().stream().collect(
                                        Collectors.toMap(Map.Entry::getKey, y -> y.getValue().get())))),
                databaseAvailable);
    }

}
