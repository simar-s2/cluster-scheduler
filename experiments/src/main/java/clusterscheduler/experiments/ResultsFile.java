package clusterscheduler.experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes an experiment's measurements to results/NAME.json. The README's
 * results table is generated from these files, never typed by hand.
 */
final class ResultsFile {
    static final Path DIRECTORY = Path.of("results");
    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private ResultsFile() {}

    static Path write(String name, Object results) {
        Path file = DIRECTORY.resolve(name + ".json");
        try {
            Files.createDirectories(DIRECTORY);
            JSON.writeValue(file.toFile(), results);
            return file;
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
