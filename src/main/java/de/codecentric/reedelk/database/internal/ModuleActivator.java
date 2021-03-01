package de.codecentric.reedelk.database.internal;


import de.codecentric.reedelk.database.internal.commons.DataSourceService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import static org.osgi.service.component.annotations.ServiceScope.SINGLETON;

@Component(service = ModuleActivator.class, scope = SINGLETON, immediate = true)
public class ModuleActivator {

    @Reference
    private DataSourceService dataSourceService;

    @Deactivate
    public void deactivate() {
        // All the connection pools should be closed, to make sure
        // that nothing has been left open.
        dataSourceService.dispose();
    }
}
