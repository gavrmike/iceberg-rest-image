package org.apache.iceberg.core;

import java.util.Date;

import org.apache.iceberg.catalog.TableIdentifier;

public interface Loggable {
    public void addLog(TableIdentifier tableIdentifier, String userId, String action, Date now);

    public void addLog(TableIdentifier tableIdentifier, String userId, String action);
}
