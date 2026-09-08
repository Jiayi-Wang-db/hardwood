/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;

/// Runs a projection and filter-shape sweep over the four `parquet-footer-bench` datasets.
///
/// Each cell reports the median of 11 runs after three warmups. `plan_ms` includes opening the
/// file and building column readers. `first_batch_ms` measures producing the first batch of at
/// most 8,192 rows, excluding reader close. Footer order rotates between cells.
///
/// Prepare an OSS, jump-table, and full modular file for every corpus entry, then run:
/// ```shell
/// java -cp target/benchmarks.jar dev.hardwood.benchmarks.FooterQuerySweep /path/to/data
/// ```
public final class FooterQuerySweep {
    private static final int WARMUPS = 3;
    private static final int RUNS = 11;
    private static final List<String> FOOTERS = List.of("oss", "jump", "modular");

    private record Dataset(String name, String filterColumn, String secondFilterColumn,
            Supplier<FilterPredicate> filter, Supplier<FilterPredicate> secondFilter) {
    }

    private record Shape(String name, boolean projectFilter, boolean excludeFilter,
            boolean twoFilters) {
    }

    private record Result(double planMs, double firstBatchMs, int records) {
    }

    private static final List<Dataset> DATASETS = List.of(
            new Dataset("us-accidents-00004-of-00007", "Severity", "State",
                    () -> FilterPredicate.gt("Severity", 3L),
                    () -> FilterPredicate.eq("State", "CA")),
            new Dataset("fineweb-10bt-000", "token_count", "language",
                    () -> FilterPredicate.gt("token_count", 5_000L),
                    () -> FilterPredicate.eq("language", "en")),
            new Dataset("hacker-news-00000-of-00039", "score", "type",
                    () -> FilterPredicate.gt("score", 100L),
                    () -> FilterPredicate.eq("type", "story")),
            new Dataset("yellow-tripdata-2025-01", "fare_amount", "passenger_count",
                    () -> FilterPredicate.gt("fare_amount", 100.0),
                    () -> FilterPredicate.gt("passenger_count", 1L)));

    private static final List<Shape> SHAPES = List.of(
            new Shape("none", false, false, false),
            new Shape("filter_projected", true, false, false),
            new Shape("filter_unprojected", false, true, false),
            new Shape("two_filters_unprojected", false, true, true));

    private FooterQuerySweep() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException(
                    "Usage: FooterQuerySweep <data-dir> [dataset-name]");
        }
        Path data = Path.of(args[0]);
        System.out.println("dataset,columns,projection,projected,shape,footer,plan_ms,"
                + "first_batch_ms,records");
        int cell = 0;
        for (Dataset dataset : DATASETS) {
            if (args.length == 2 && !dataset.name().equals(args[1])) {
                continue;
            }
            List<String> columns;
            try (ParquetFileReader reader = open(path(data, dataset.name(), "oss"))) {
                columns = reader.getFileSchema().getColumns().stream()
                        .map(column -> column.fieldPath().toString()).toList();
            }
            int[] widths = {1, Math.max(1, (columns.size() + 9) / 10),
                    Math.max(1, (columns.size() + 1) / 2), columns.size()};
            String[] labels = {"1", "10pct", "50pct", "100pct"};
            for (int width = 0; width < widths.length; width++) {
                for (Shape shape : SHAPES) {
                    if (shape.excludeFilter() && widths[width] > columns.size()
                            - (shape.twoFilters() ? 2 : 1)) {
                        continue;
                    }
                    String[] projection = projection(columns, widths[width], dataset, shape);
                    FilterPredicate filter = predicate(dataset, shape);
                    for (int footerIndex = 0; footerIndex < FOOTERS.size(); footerIndex++) {
                        String footer = FOOTERS.get((cell + footerIndex) % FOOTERS.size());
                        Result result = measure(path(data, dataset.name(), footer),
                                projection, filter);
                        System.out.printf(Locale.ROOT,
                                "%s,%d,%s,%d,%s,%s,%.3f,%.3f,%d%n",
                                dataset.name(), columns.size(), labels[width], projection.length,
                                shape.name(), footer, result.planMs(), result.firstBatchMs(),
                                result.records());
                    }
                    cell++;
                }
            }
        }
    }

    private static Result measure(Path path, String[] projection, FilterPredicate filter)
            throws Exception {
        for (int i = 0; i < WARMUPS; i++) {
            run(path, projection, filter);
        }
        List<Double> plans = new ArrayList<>();
        List<Double> batches = new ArrayList<>();
        int expectedRecords = -1;
        for (int i = 0; i < RUNS; i++) {
            Result result = run(path, projection, filter);
            plans.add(result.planMs());
            batches.add(result.firstBatchMs());
            if (expectedRecords >= 0 && result.records() != expectedRecords) {
                throw new AssertionError("Record count changed for " + path);
            }
            expectedRecords = result.records();
        }
        Collections.sort(plans);
        Collections.sort(batches);
        return new Result(plans.get(RUNS / 2), batches.get(RUNS / 2), expectedRecords);
    }

    private static Result run(Path path, String[] projection, FilterPredicate filter)
            throws Exception {
        long start = System.nanoTime();
        long planned;
        long finished;
        int records;
        try (ParquetFileReader file = open(path)) {
            var builder = file.buildColumnReaders(ColumnProjection.columns(projection));
            if (filter != null) {
                builder.filter(filter);
            }
            try (ColumnReaders readers = builder.batchSize(8_192).build()) {
                planned = System.nanoTime();
                records = readers.nextBatch() ? readers.getRecordCount() : 0;
                finished = System.nanoTime();
            }
        }
        return new Result((planned - start) / 1_000_000.0,
                (finished - planned) / 1_000_000.0, records);
    }

    private static String[] projection(List<String> columns, int width, Dataset dataset,
            Shape shape) {
        List<String> candidates = new ArrayList<>(columns);
        if (shape.excludeFilter() || shape.projectFilter()) {
            candidates.remove(dataset.filterColumn());
        }
        if (shape.twoFilters()) {
            candidates.remove(dataset.secondFilterColumn());
        }
        List<String> selected = new ArrayList<>(candidates.subList(0,
                Math.min(width, candidates.size())));
        if (shape.projectFilter()) {
            if (selected.size() == width) {
                selected.removeLast();
            }
            selected.add(dataset.filterColumn());
        }
        return selected.toArray(String[]::new);
    }

    private static FilterPredicate predicate(Dataset dataset, Shape shape) {
        if (shape.name().equals("none")) {
            return null;
        }
        return shape.twoFilters()
                ? FilterPredicate.and(dataset.filter().get(), dataset.secondFilter().get())
                : dataset.filter().get();
    }

    private static ParquetFileReader open(Path path) throws Exception {
        return ParquetFileReader.open(InputFile.of(path));
    }

    private static Path path(Path data, String dataset, String footer) {
        return switch (footer) {
            case "oss" -> data.resolve(dataset + ".parquet");
            case "jump" -> data.resolve("decode-bench").resolve(dataset + ".jt.parquet");
            case "modular" -> data.resolve(dataset + ".modular.parquet");
            default -> throw new IllegalArgumentException(footer);
        };
    }
}
