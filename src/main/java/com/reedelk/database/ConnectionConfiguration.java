package com.reedelk.database;

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
    @Property("Password")
    private String password;
    @Property("Driver")
    @Default("com.mysql.cj.jdbc.Driver")
    private String driverClass;

    public String getId() {
        return id;
    }

    public String getConnectionURL() {
        return connectionURL;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }
}