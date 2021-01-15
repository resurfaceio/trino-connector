// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class LocalFileModule implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(LocalFileConfig.class);
        binder.bind(LocalFileConnector.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileTables.class).in(Scopes.SINGLETON);
    }

}
