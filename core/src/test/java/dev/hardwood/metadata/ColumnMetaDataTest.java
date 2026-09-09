/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnMetaDataTest {

    @Test
    void lazyStatisticsAreResolvedOnlyWhenAccessed() {
        AtomicInteger resolutions = new AtomicInteger();
        Statistics expected = new Statistics(new byte[] {1}, new byte[] {2}, 3L, null, false);
        ColumnMetaData metadata = ColumnMetaData.withLazyStatistics(
                PhysicalType.INT32, List.of(Encoding.PLAIN), FieldPath.of("value"),
                CompressionCodec.UNCOMPRESSED, 10, 20, 15, Map.of(), 100, null,
                () -> {
                    resolutions.incrementAndGet();
                    return expected;
                },
                null, null, null, List.of(), null);

        assertThat(metadata.dataPageOffset()).isEqualTo(100);
        assertThat(metadata.totalCompressedSize()).isEqualTo(15);
        assertThat(resolutions).hasValue(0);

        assertThat(metadata.statistics()).isSameAs(expected);
        assertThat(metadata.statistics()).isSameAs(expected);
        assertThat(resolutions).hasValue(1);
    }
}
