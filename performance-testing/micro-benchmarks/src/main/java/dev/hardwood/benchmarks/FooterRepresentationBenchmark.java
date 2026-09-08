/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;

/// Compares the same US Accidents data pages through three footer representations: ordinary
/// Parquet (`oss`), an indexed ordinary footer (`jump`), and modular metadata (`modular`).
///
/// The query projects and filters `Severity`, returning 30,208 rows with a sum of 120,832. Both
/// `open` measures the representation's projection-independent schema/directory work. The complete
/// filtered query includes preparing metadata for the resolved projection and filter columns, then
/// executing the same data-page scan for every representation.
///
/// Generate the corpus and derived files in `parquet-footer-bench`, then run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
///   FooterRepresentationBenchmark -p dataDir=/path/to/parquet-footer-bench/real-footer-size/data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class FooterRepresentationBenchmark {

    private static final String DATASET = "us-accidents-00004-of-00007";

    @Param({})
    private String dataDir;

    @Param({ "oss", "jump", "modular" })
    private String footer;

    private Path path;

    @Setup
    public void setup() {
        Path directory = Path.of(dataDir).toAbsolutePath().normalize();
        path = switch (footer) {
            case "oss" -> directory.resolve(DATASET + ".parquet");
            case "jump" -> directory.resolve("decode-bench").resolve(DATASET + ".jt.parquet");
            case "modular" -> directory.resolve(DATASET + ".modular.parquet");
            default -> throw new IllegalStateException("Unknown footer representation: " + footer);
        };
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException("Benchmark file not found: " + path);
        }
    }

    @Benchmark
    public void open(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            blackhole.consume(reader.getFileSchema().getColumnCount());
        }
    }

    @Benchmark
    public void filteredQuery(Blackhole blackhole) throws IOException {
        QueryResult result = query(path);
        blackhole.consume(result.rows);
        blackhole.consume(result.sum);
    }

    static QueryResult query(Path path) throws IOException {
        long rows = 0;
        long sum = 0;
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(path));
             ColumnReaders columns = file.buildColumnReaders(
                     ColumnProjection.columns("Severity"))
                     .filter(FilterPredicate.gt("Severity", 3L))
                     .build()) {
            while (columns.nextBatch()) {
                int count = columns.getRecordCount();
                long[] values = columns.getColumnReader("Severity").getLongs();
                for (int i = 0; i < count; i++) {
                    rows++;
                    sum += values[i];
                }
            }
        }
        return new QueryResult(rows, sum);
    }

    record QueryResult(long rows, long sum) {
    }
}
