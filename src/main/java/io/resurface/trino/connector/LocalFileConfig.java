// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;

public class LocalFileConfig {

    private String httpRequestLogLocation = "var/log/http-request.log";
    private String httpRequestLogFileNamePattern;

    @FileExists
    public String getHttpRequestLogLocation() {
        return httpRequestLogLocation;
    }

    @Config("trino-logs.http-request-log.location")
    @LegacyConfig("presto-logs.http-request-log.location")
    @ConfigDescription("Directory or file where http request logs are written")
    public LocalFileConfig setHttpRequestLogLocation(String httpRequestLogLocation) {
        this.httpRequestLogLocation = httpRequestLogLocation;
        return this;
    }

    public String getHttpRequestLogFileNamePattern() {
        return httpRequestLogFileNamePattern;
    }

    @Config("trino-logs.http-request-log.pattern")
    @LegacyConfig("presto-logs.http-request-log.pattern")
    @ConfigDescription("If log location is a directory this glob is used to match the file names in the directory")
    public LocalFileConfig setHttpRequestLogFileNamePattern(String pattern) {
        this.httpRequestLogFileNamePattern = pattern;
        return this;
    }

}
