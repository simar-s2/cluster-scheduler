package clusterscheduler.experiments;

/** Experiment parameters, read from environment variables with the defaults used for the README. */
final class Settings {
    private Settings() {}

    static int number(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }
}
