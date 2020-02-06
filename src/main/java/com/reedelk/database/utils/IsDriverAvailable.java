package com.reedelk.database.utils;

import com.reedelk.database.commons.DatabaseDriver;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class IsDriverAvailable {

    public static boolean of(String driverClass) {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverClass)) {
                return true;
            }
        }
        return false;
    }

    public static boolean of(DatabaseDriver databaseDriver) {
        return of(databaseDriver.qualifiedName());
    }
}
