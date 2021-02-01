// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.configuration.Config;

public class ResurfaceConfig {

    private String messagesDir = null;

    public String getMessagesDir() {
        return messagesDir;
    }

    @Config("resurface.messages.dir")
    public ResurfaceConfig setMessagesDir(String messagesDir) {
        this.messagesDir = messagesDir;
        return this;
    }

}
