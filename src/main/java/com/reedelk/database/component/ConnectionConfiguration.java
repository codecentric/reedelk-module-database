package com.reedelk.database.component;

import com.reedelk.database.commons.DatabaseDriver;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

@Shared
@Component(service = ConnectionConfiguration.class, scope = ServiceScope.PROTOTYPE)
public class ConnectionConfiguration implements Implementor {

    @Property("id")
    @Hidden
    private String id;

    @Property("Connection URL")
    @Example("<ul>" +
            "<li>H2: jdbc:h2:~/test</li>" +
            "<li>MySQL: jdbc:mysql://localhost:3306/mydatabase</li>" +
            "<li>Oracle: jdbc:oracle:thin:@localhost:1521:orcl</li>" +
            "<li>PostgreSQL: jdbc:postgresql://host:port/database</li>" +
            "</ul>")
    @Hint("jdbc:mysql://localhost:3306/mydatabase")
    @InitValue("jdbc:mysql://localhost:3306/mydatabase")
    @Description("The connection URL is a string that a JDBC driver uses to connect to a database. " +
            "It can contain information such as where to search for the database, " +
            "the name of the database to connect to, and configuration properties.")
    private String connectionURL;

    @Property("Username")
    @Example("myDatabaseUser")
    @Description("The username to be used to create the database connection.")
    private String username;

    @Property("Password")
    @Password
    @Example("myDatabasePassword")
    @Description("The password to be used to create the database connection.")
    private String password;

    @Example("ORACLE")
    @InitValue("MYSQL")
    @Property("Driver")
    @Description("The fully qualified name of the JDBC database driver class. " +
            "The JDBC drivers must be present in the {RUNTIME_HOME}/lib directory.")
    private DatabaseDriver databaseDriver;

    @Property("Min Pool Size")
    @Hint("3")
    @Example("5")
    @DefaultValue("3")
    @Description("Minimum number of Connections the connection pool will maintain at any given time.")
    private Integer minPoolSize;

    @Property("Max Pool Size")
    @Hint("15")
    @Example("20")
    @DefaultValue("15")
    @Description("Maximum number of Connections the connection pool will maintain at any given time.")
    private Integer maxPoolSize;

    @Property("Acquire Increment")
    @Hint("3")
    @Example("5")
    @DefaultValue("3")
    @Description("Determines how many connections at a time the connection pool will try to acquire " +
            "when the pool is exhausted.")
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
