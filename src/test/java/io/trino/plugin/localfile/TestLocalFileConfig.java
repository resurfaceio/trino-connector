// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestLocalFileConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LocalFileConfig.class)
                .setHttpRequestLogLocation("var/log/http-request.log")
                .setHttpRequestLogFileNamePattern(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path httpRequestLogFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("trino-logs.http-request-log.location", httpRequestLogFile.toString())
                .put("trino-logs.http-request-log.pattern", "bar")
                .build();

        LocalFileConfig expected = new LocalFileConfig()
                .setHttpRequestLogLocation(httpRequestLogFile.toString())
                .setHttpRequestLogFileNamePattern("bar");

        assertFullMapping(properties, expected);
    }
}
