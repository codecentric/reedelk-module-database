package com.reedelk.database;


import com.reedelk.database.commons.DataSourceService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import static org.osgi.service.component.annotations.ServiceScope.SINGLETON;

@Component(service = DatabaseModuleActivator.class, scope = SINGLETON, immediate = true)
public class DatabaseModuleActivator {

    @Reference
    private DataSourceService dataSourceService;

    @Deactivate
    public void deactivate() {
        // All the connection pools should be closed, to make sure
        // that nothing has been left open.
        dataSourceService.dispose();
    }
}
