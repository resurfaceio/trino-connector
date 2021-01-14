// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.localfile.LocalFileErrorCode.LOCAL_FILE_FILESYSTEM_ERROR;
import static io.trino.plugin.localfile.LocalFileErrorCode.LOCAL_FILE_NO_FILES;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;

final class DataLocation
{
    private final File location;
    private final Optional<String> pattern;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @JsonCreator
    public DataLocation(
            @JsonProperty("location") String location,
            @JsonProperty("pattern") Optional<String> pattern)
    {
        requireNonNull(location, "location is null");
        requireNonNull(pattern, "pattern is null");

        File file = new File(location);
        if (!file.exists() && pattern.isPresent()) {
            file.mkdirs();
        }

        checkArgument(file.exists(), "location does not exist");
        if (pattern.isPresent() && !file.isDirectory()) {
            throw new IllegalArgumentException("pattern may be specified only if location is a directory");
        }

        this.location = file;
        this.pattern = (pattern.isEmpty() && file.isDirectory()) ? Optional.of("*") : pattern;
    }

    @JsonProperty
    public File getLocation()
    {
        return location;
    }

    @JsonProperty
    public Optional<String> getPattern()
    {
        return pattern;
    }

    public List<File> files()
    {
        checkState(location.exists(), "location %s doesn't exist", location);
        if (pattern.isEmpty()) {
            return ImmutableList.of(location);
        }

        checkState(location.isDirectory(), "location %s is not a directory", location);

        try (DirectoryStream<Path> paths = newDirectoryStream(location.toPath(), pattern.get())) {
            ImmutableList.Builder<File> builder = ImmutableList.builder();
            for (Path path : paths) {
                builder.add(path.toFile());
            }
            List<File> files = builder.build();

            if (files.isEmpty()) {
                throw new TrinoException(LOCAL_FILE_NO_FILES, "No matching files found in directory: " + location);
            }
            return files.stream()
                    .sorted((o1, o2) -> Long.compare(o2.lastModified(), o1.lastModified()))
                    .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new TrinoException(LOCAL_FILE_FILESYSTEM_ERROR, "Error listing files in directory: " + location, e);
        }
    }
}
