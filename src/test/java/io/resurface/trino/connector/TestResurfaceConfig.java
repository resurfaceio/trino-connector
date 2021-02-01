// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.*;

public class TestResurfaceConfig {

    @Test
    public void testDefaults() {
        assertRecordedDefaults(recordDefaults(ResurfaceConfig.class).setMessagesDir(null));
    }

    @Test
    public void testExplicitPropertyMappings() throws IOException {
        Path tmpfile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("resurface.messages.dir", tmpfile.toString())
                .build();

        ResurfaceConfig expected = new ResurfaceConfig().setMessagesDir(tmpfile.toString());
        assertFullMapping(properties, expected);
    }

}
