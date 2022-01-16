// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.configuration.Config;

public class ResurfaceConfig {

    private String messagesDir = null;
    private int messagesSlabs;
    private String viewsDir = null;

    public String getMessagesDir() {
        return messagesDir;
    }

    public int getMessagesSlabs() {
        return messagesSlabs;
    }

    public String getViewsDir() {
        return viewsDir;
    }

    @Config("resurface.messages.dir")
    public ResurfaceConfig setMessagesDir(String dir) {
        this.messagesDir = dir;
        return this;
    }

    @Config("resurface.messages.slabs")
    public ResurfaceConfig setMessagesSlabs(int messagesSlabs) {
        this.messagesSlabs = messagesSlabs;
        return this;
    }

    @Config("resurface.views.dir")
    public ResurfaceConfig setViewsDir(String dir) {
        this.viewsDir = dir;
        return this;
    }

}
