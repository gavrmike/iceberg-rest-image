package org.apache.iceberg.core;

public interface CheckUser {
    public boolean checkUser(String userId, String password);

    public String getPasswordHash(String userId);

    public void addUser(String userId, String password);

    public boolean isAuthRequired();
}
