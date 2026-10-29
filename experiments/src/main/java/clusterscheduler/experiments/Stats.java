package clusterscheduler.experiments;

import java.util.List;

/** Summary numbers for a list of measurements. */
record Stats(int count, double min, double median, double p99, double max, double mean) {

    static Stats of(List<? extends Number> values) {
        if (values.isEmpty()) {
            return new Stats(0, 0, 0, 0, 0, 0);
        }
        List<Double> sorted = values.stream().map(Number::doubleValue).sorted().toList();
        double sum = sorted.stream().mapToDouble(Double::doubleValue).sum();
        return new Stats(
                sorted.size(),
                sorted.get(0),
                percentile(sorted, 50),
                percentile(sorted, 99),
                sorted.get(sorted.size() - 1),
                sum / sorted.size());
    }

    /** Nearest-rank percentile: the smallest value with at least p% of values at or below it. */
    static double percentile(List<Double> sortedValues, double percent) {
        int rank = (int) Math.ceil(percent / 100.0 * sortedValues.size());
        return sortedValues.get(Math.max(rank, 1) - 1);
    }
}
