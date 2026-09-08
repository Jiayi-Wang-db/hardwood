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
import java.util.Objects;
import java.util.function.Supplier;

/// Metadata for a column chunk.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/data-pages/columnchunks/">File Format - Column Chunks</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public final class ColumnMetaData {
    private final PhysicalType type;
    private final List<Encoding> encodings;
    private final FieldPath pathInSchema;
    private final CompressionCodec codec;
    private final long numValues;
    private final long totalUncompressedSize;
    private final long totalCompressedSize;
    private final Map<String, String> keyValueMetadata;
    private final long dataPageOffset;
    private final Long dictionaryPageOffset;
    private volatile Statistics statistics;
    private Supplier<Statistics> statisticsSupplier;
    private volatile boolean statisticsResolved;
    private final GeospatialStatistics geospatialStatistics;
    private final Long bloomFilterOffset;
    private final Integer bloomFilterLength;
    private final List<PageEncodingStats> encodingStats;
    private final SizeStatistics sizeStatistics;

    public ColumnMetaData(PhysicalType type, List<Encoding> encodings, FieldPath pathInSchema,
            CompressionCodec codec, long numValues, long totalUncompressedSize,
            long totalCompressedSize, Map<String, String> keyValueMetadata, long dataPageOffset,
            Long dictionaryPageOffset, Statistics statistics,
            GeospatialStatistics geospatialStatistics, Long bloomFilterOffset,
            Integer bloomFilterLength, List<PageEncodingStats> encodingStats,
            SizeStatistics sizeStatistics) {
        this(type, encodings, pathInSchema, codec, numValues, totalUncompressedSize,
                totalCompressedSize, keyValueMetadata, dataPageOffset, dictionaryPageOffset,
                statistics, null, true, geospatialStatistics, bloomFilterOffset,
                bloomFilterLength, encodingStats, sizeStatistics);
    }

    public static ColumnMetaData withLazyStatistics(PhysicalType type, List<Encoding> encodings,
            FieldPath pathInSchema, CompressionCodec codec, long numValues,
            long totalUncompressedSize, long totalCompressedSize,
            Map<String, String> keyValueMetadata, long dataPageOffset, Long dictionaryPageOffset,
            Supplier<Statistics> statisticsSupplier, GeospatialStatistics geospatialStatistics,
            Long bloomFilterOffset, Integer bloomFilterLength,
            List<PageEncodingStats> encodingStats, SizeStatistics sizeStatistics) {
        return new ColumnMetaData(type, encodings, pathInSchema, codec, numValues,
                totalUncompressedSize, totalCompressedSize, keyValueMetadata, dataPageOffset,
                dictionaryPageOffset, null, Objects.requireNonNull(statisticsSupplier), false,
                geospatialStatistics, bloomFilterOffset, bloomFilterLength, encodingStats,
                sizeStatistics);
    }

    private ColumnMetaData(PhysicalType type, List<Encoding> encodings, FieldPath pathInSchema,
            CompressionCodec codec, long numValues, long totalUncompressedSize,
            long totalCompressedSize, Map<String, String> keyValueMetadata, long dataPageOffset,
            Long dictionaryPageOffset, Statistics statistics,
            Supplier<Statistics> statisticsSupplier, boolean statisticsResolved,
            GeospatialStatistics geospatialStatistics, Long bloomFilterOffset,
            Integer bloomFilterLength, List<PageEncodingStats> encodingStats,
            SizeStatistics sizeStatistics) {
        this.type = type;
        this.encodings = encodings;
        this.pathInSchema = pathInSchema;
        this.codec = codec;
        this.numValues = numValues;
        this.totalUncompressedSize = totalUncompressedSize;
        this.totalCompressedSize = totalCompressedSize;
        this.keyValueMetadata = keyValueMetadata;
        this.dataPageOffset = dataPageOffset;
        this.dictionaryPageOffset = dictionaryPageOffset;
        this.statistics = statistics;
        this.statisticsSupplier = statisticsSupplier;
        this.statisticsResolved = statisticsResolved;
        this.geospatialStatistics = geospatialStatistics;
        this.bloomFilterOffset = bloomFilterOffset;
        this.bloomFilterLength = bloomFilterLength;
        this.encodingStats = encodingStats;
        this.sizeStatistics = sizeStatistics;
    }

    public PhysicalType type() {
        return type;
    }

    public List<Encoding> encodings() {
        return encodings;
    }

    public FieldPath pathInSchema() {
        return pathInSchema;
    }

    public CompressionCodec codec() {
        return codec;
    }

    public long numValues() {
        return numValues;
    }

    public long totalUncompressedSize() {
        return totalUncompressedSize;
    }

    public long totalCompressedSize() {
        return totalCompressedSize;
    }

    public Map<String, String> keyValueMetadata() {
        return keyValueMetadata;
    }

    public long dataPageOffset() {
        return dataPageOffset;
    }

    public Long dictionaryPageOffset() {
        return dictionaryPageOffset;
    }

    public Statistics statistics() {
        if (!statisticsResolved) {
            synchronized (this) {
                if (!statisticsResolved) {
                    statistics = statisticsSupplier.get();
                    statisticsSupplier = null;
                    statisticsResolved = true;
                }
            }
        }
        return statistics;
    }

    public GeospatialStatistics geospatialStatistics() {
        return geospatialStatistics;
    }

    public Long bloomFilterOffset() {
        return bloomFilterOffset;
    }

    public Integer bloomFilterLength() {
        return bloomFilterLength;
    }

    public List<PageEncodingStats> encodingStats() {
        return encodingStats;
    }

    public SizeStatistics sizeStatistics() {
        return sizeStatistics;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ColumnMetaData that)) {
            return false;
        }
        return numValues == that.numValues
                && totalUncompressedSize == that.totalUncompressedSize
                && totalCompressedSize == that.totalCompressedSize
                && dataPageOffset == that.dataPageOffset && type == that.type
                && Objects.equals(encodings, that.encodings)
                && Objects.equals(pathInSchema, that.pathInSchema) && codec == that.codec
                && Objects.equals(keyValueMetadata, that.keyValueMetadata)
                && Objects.equals(dictionaryPageOffset, that.dictionaryPageOffset)
                && Objects.equals(statistics(), that.statistics())
                && Objects.equals(geospatialStatistics, that.geospatialStatistics)
                && Objects.equals(bloomFilterOffset, that.bloomFilterOffset)
                && Objects.equals(bloomFilterLength, that.bloomFilterLength)
                && Objects.equals(encodingStats, that.encodingStats)
                && Objects.equals(sizeStatistics, that.sizeStatistics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, encodings, pathInSchema, codec, numValues,
                totalUncompressedSize, totalCompressedSize, keyValueMetadata, dataPageOffset,
                dictionaryPageOffset, statistics(), geospatialStatistics, bloomFilterOffset,
                bloomFilterLength, encodingStats, sizeStatistics);
    }

    @Override
    public String toString() {
        return "ColumnMetaData[type=" + type + ", encodings=" + encodings
                + ", pathInSchema=" + pathInSchema + ", codec=" + codec
                + ", numValues=" + numValues + ", totalUncompressedSize="
                + totalUncompressedSize + ", totalCompressedSize=" + totalCompressedSize
                + ", keyValueMetadata=" + keyValueMetadata + ", dataPageOffset="
                + dataPageOffset + ", dictionaryPageOffset=" + dictionaryPageOffset
                + ", statistics=" + statistics() + ", geospatialStatistics="
                + geospatialStatistics + ", bloomFilterOffset=" + bloomFilterOffset
                + ", bloomFilterLength=" + bloomFilterLength + ", encodingStats="
                + encodingStats + ", sizeStatistics=" + sizeStatistics + "]";
    }
}
