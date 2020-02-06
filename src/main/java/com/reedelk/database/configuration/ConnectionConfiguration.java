package com.reedelk.database.configuration;

import com.reedelk.database.commons.DatabaseDriver;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@Shared
@Component(service = ConnectionConfiguration.class, scope = ServiceScope.PROTOTYPE)
public class ConnectionConfiguration implements Implementor {

    @Hidden
    @Property("id")
    private String id;

    @Property("Connection URL")
    @Hint("jdbc:mysql://localhost:3306/mydatabase")
    @Default("jdbc:mysql://localhost:3306/mydatabase")
    private String connectionURL;

    @Property("Username")
    private String username;

    @Password
    @Property("Password")
    private String password;

    @Property("Driver")
    @Default("MYSQL")
    private DatabaseDriver databaseDriver;

    @Hint("3")
    @Property("Min Pool Size")
    private Integer minPoolSize;

    @Hint("20")
    @Property("Max Pool Size")
    private Integer maxPoolSize;

    @Hint("5")
    @Property("Acquire Increment")
    private Integer acquireIncrement;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DatabaseDriver getDatabaseDriver() {
        return databaseDriver;
    }

    public void setDatabaseDriver(DatabaseDriver databaseDriver) {
        this.databaseDriver = databaseDriver;
    }

    public Integer getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(Integer minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public Integer getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(Integer maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public Integer getAcquireIncrement() {
        return acquireIncrement;
    }

    public void setAcquireIncrement(Integer acquireIncrement) {
        this.acquireIncrement = acquireIncrement;
    }
}
