// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ResurfaceModule implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(ResurfaceConfig.class);
        binder.bind(ResurfaceConnector.class).in(Scopes.SINGLETON);
        binder.bind(ResurfaceMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ResurfaceSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ResurfaceRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ResurfaceHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ResurfaceTables.class).in(Scopes.SINGLETON);
    }

}
