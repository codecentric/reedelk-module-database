package com.reedelk.mysql;


import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;

import java.sql.DriverManager;

import static org.osgi.service.component.annotations.ServiceScope.SINGLETON;

@Component(service = DatabaseModuleActivator.class, scope = SINGLETON, immediate = true)
public class DatabaseModuleActivator {

    @Activate
    public void activate(BundleContext context) throws BundleException {
        // Force java to load the drivers. otherwise C3PO does not find it
        // by using Class.forName and it logs a warn message which is not true,
        // because the drivers are effectively loaded when using the javax.sql API.
        DriverManager.getDrivers();
        // TODO: Log the drivers found here from this list.
    }

    @Deactivate
    public void deactivate() {
        // TODO: The connection pools should be cleaned up just to make sure that
        //  nothing is left open.

    }
}
