/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.nio.file.Path;

/// Correctness gate for [FooterRepresentationBenchmark].
public final class FooterRepresentationGate {

    private FooterRepresentationGate() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: FooterRepresentationGate <data-dir>");
        }
        Path directory = Path.of(args[0]).toAbsolutePath().normalize();
        String dataset = "us-accidents-00004-of-00007";
        Path[] paths = {
                directory.resolve(dataset + ".parquet"),
                directory.resolve("decode-bench").resolve(dataset + ".jt.parquet"),
                directory.resolve(dataset + ".modular.parquet")
        };
        FooterRepresentationBenchmark.QueryResult expected = null;
        for (Path path : paths) {
            FooterRepresentationBenchmark.QueryResult actual =
                    FooterRepresentationBenchmark.query(path);
            if (expected == null) {
                expected = actual;
            }
            else if (!expected.equals(actual)) {
                throw new AssertionError(path + " produced " + actual + ", expected " + expected);
            }
            System.out.println(path.getFileName() + " " + actual);
        }
        if (expected.rows() != 30_208 || expected.sum() != 120_832) {
            throw new AssertionError("Unexpected corpus result: " + expected);
        }
    }
}
