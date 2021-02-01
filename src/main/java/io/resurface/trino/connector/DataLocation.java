// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.resurface.trino.connector.ResurfaceErrorCode.RESURFACE_FILESYSTEM_ERROR;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;

final class DataLocation {

    @JsonCreator
    public DataLocation(@JsonProperty("location") String location) {
        requireNonNull(location, "location is null");
        this.location = new File(location);
    }

    private final File location;

    public List<File> files() {
        checkState(location.exists(), "location %s doesn't exist", location);
        checkState(location.isDirectory(), "location %s is not a directory", location);
        try (DirectoryStream<Path> paths = newDirectoryStream(location.toPath())) {
            ImmutableList.Builder<File> builder = ImmutableList.builder();
            for (Path path : paths) builder.add(path.toFile());
            List<File> files = builder.build();
            return files.isEmpty() ? new ArrayList<>() : new ArrayList<>(files);
        } catch (IOException e) {
            throw new TrinoException(RESURFACE_FILESYSTEM_ERROR, "Error listing files in directory: " + location, e);
        }
    }

}
