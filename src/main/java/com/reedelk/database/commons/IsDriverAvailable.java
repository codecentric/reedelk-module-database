package com.reedelk.database.commons;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

class IsDriverAvailable {

    private static boolean of(String driverClass) {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverClass)) {
                return true;
            }
        }
        return false;
    }

    static boolean of(DatabaseDriver databaseDriver) {
        return of(databaseDriver.qualifiedName());
    }
}
