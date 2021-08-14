// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.configuration.Config;

public class ResurfaceConfig {

    private String messagesDir = null;
    private String viewsDir = null;

    public String getMessagesDir() {
        return messagesDir;
    }

    public String getViewsDir() {
        return viewsDir;
    }

    @Config("resurface.messages.dir")
    public ResurfaceConfig setMessagesDir(String dir) {
        this.messagesDir = dir;
        return this;
    }

    @Config("resurface.views.dir")
    public ResurfaceConfig setViewsDir(String dir) {
        this.viewsDir = dir;
        return this;
    }

}
