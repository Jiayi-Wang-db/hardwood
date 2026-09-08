/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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
/// The default `pruned` mode uses predicates above each column's maximum, producing complete
/// zero-row queries without data-page reads. The `selective` mode uses real predicates that prune
/// most row groups and scan the survivors. `query_ms` includes footer open, planning, and consuming
/// all results, excluding reader close. Footer order rotates between cells.
///
/// Prepare an OSS, jump-table, and full modular file for every corpus entry, then run:
/// ```shell
/// java -Dfooter.sweep.mode=selective -Dfooter.sweep.output=results.csv \
///   -cp target/benchmarks.jar \
///   dev.hardwood.benchmarks.FooterQuerySweep /path/to/data
/// ```
public final class FooterQuerySweep {
    private static final int PRUNED_WARMUPS = 3;
    private static final int PRUNED_RUNS = 11;
    private static final int SELECTIVE_WARMUPS = 1;
    private static final int SELECTIVE_RUNS = 5;
    private static final List<String> FOOTERS = List.of("oss", "jump", "modular");

    private record Dataset(String name, String filterColumn, String secondFilterColumn,
            Supplier<FilterPredicate> pruningFilter,
            Supplier<FilterPredicate> secondPruningFilter,
            Supplier<FilterPredicate> selectiveFilter) {
    }

    private record Shape(String name, boolean projectFilter, boolean excludeFilter,
            boolean twoFilters) {
    }

    private record Result(double planMs, double queryMs, long records) {
    }

    private static final List<Dataset> DATASETS = List.of(
            new Dataset("us-accidents-00004-of-00007", "Start_Lat", "Start_Lng",
                    () -> FilterPredicate.gt("Start_Lat", Double.MAX_VALUE),
                    () -> FilterPredicate.gt("Start_Lng", Double.MAX_VALUE),
                    () -> FilterPredicate.eq("Start_Lat", 49.00049329)),
            new Dataset("fineweb-10bt-000", "token_count", "language_score",
                    () -> FilterPredicate.gt("token_count", Long.MAX_VALUE),
                    () -> FilterPredicate.gt("language_score", Double.MAX_VALUE),
                    () -> FilterPredicate.eq("token_count", 130_040L)),
            new Dataset("hacker-news-00000-of-00039", "id", "score",
                    () -> FilterPredicate.gt("id", Long.MAX_VALUE),
                    () -> FilterPredicate.gt("score", Long.MAX_VALUE),
                    () -> FilterPredicate.eq("id", 1_072_138L)),
            new Dataset("yellow-tripdata-2025-01", "fare_amount", "trip_distance",
                    () -> FilterPredicate.gt("fare_amount", Double.MAX_VALUE),
                    () -> FilterPredicate.gt("trip_distance", Double.MAX_VALUE),
                    () -> FilterPredicate.eq("fare_amount", 863_372.12)));

    private static final List<Shape> SHAPES = List.of(
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
        String mode = System.getProperty("footer.sweep.mode", "pruned");
        boolean selective = switch (mode) {
            case "pruned" -> false;
            case "selective" -> true;
            default -> throw new IllegalArgumentException("Unknown footer.sweep.mode: " + mode);
        };
        int warmups = selective ? SELECTIVE_WARMUPS : PRUNED_WARMUPS;
        int runs = selective ? SELECTIVE_RUNS : PRUNED_RUNS;
        List<Shape> shapes = selective ? List.of(SHAPES.get(1)) : SHAPES;
        String outputPath = System.getProperty("footer.sweep.output");
        PrintStream output = outputPath == null
                ? System.out
                : new PrintStream(outputPath, StandardCharsets.UTF_8);
        output.println("query_kind,dataset,columns,projection,projected,shape,filter_columns,"
                + "filters_projected,footer,plan_ms,query_ms,records");
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
                for (Shape shape : shapes) {
                    String[] projection = projection(columns, widths[width], dataset, shape);
                    FilterPredicate filter = predicate(dataset, shape, selective);
                    int projectedFilters = shape.projectFilter() ? 1 : 0;
                    String filterNames = dataset.filterColumn()
                            + (shape.twoFilters() ? "+" + dataset.secondFilterColumn() : "");
                    for (int footerIndex = 0; footerIndex < FOOTERS.size(); footerIndex++) {
                        String footer = FOOTERS.get((cell + footerIndex) % FOOTERS.size());
                        Result result = measure(path(data, dataset.name(), footer),
                                projection, filter, warmups, runs, !selective);
                        output.printf(Locale.ROOT,
                                "%s,%s,%d,%s,%d,%s,%s,%d,%s,%.3f,%.3f,%d%n",
                                mode, dataset.name(), columns.size(), labels[width],
                                projection.length, shape.name(), filterNames, projectedFilters,
                                footer,
                                result.planMs(), result.queryMs(), result.records());
                    }
                    cell++;
                }
            }
        }
        if (output != System.out) {
            output.close();
        }
    }

    private static Result measure(Path path, String[] projection, FilterPredicate filter,
            int warmups, int runs, boolean expectNoRows) throws Exception {
        for (int i = 0; i < warmups; i++) {
            run(path, projection, filter);
        }
        List<Double> plans = new ArrayList<>();
        List<Double> queries = new ArrayList<>();
        long expectedRecords = -1;
        for (int i = 0; i < runs; i++) {
            Result result = run(path, projection, filter);
            plans.add(result.planMs());
            queries.add(result.queryMs());
            if (expectedRecords >= 0 && result.records() != expectedRecords) {
                throw new AssertionError("Record count changed for " + path);
            }
            if (expectNoRows && result.records() != 0) {
                throw new AssertionError("Expected statistics to prune every row in " + path);
            }
            expectedRecords = result.records();
        }
        Collections.sort(plans);
        Collections.sort(queries);
        return new Result(plans.get(runs / 2), queries.get(runs / 2), expectedRecords);
    }

    private static Result run(Path path, String[] projection, FilterPredicate filter)
            throws Exception {
        long start = System.nanoTime();
        long planned;
        long finished;
        long records = 0;
        try (ParquetFileReader file = open(path)) {
            var builder = file.buildColumnReaders(ColumnProjection.columns(projection));
            if (filter != null) {
                builder.filter(filter);
            }
            try (ColumnReaders readers = builder.batchSize(8_192).build()) {
                planned = System.nanoTime();
                while (readers.nextBatch()) {
                    records += readers.getRecordCount();
                }
                finished = System.nanoTime();
            }
        }
        return new Result((planned - start) / 1_000_000.0,
                (finished - start) / 1_000_000.0, records);
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

    private static FilterPredicate predicate(Dataset dataset, Shape shape, boolean selective) {
        if (selective) {
            return dataset.selectiveFilter().get();
        }
        return shape.twoFilters()
                ? FilterPredicate.or(
                        dataset.pruningFilter().get(), dataset.secondPruningFilter().get())
                : dataset.pruningFilter().get();
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
