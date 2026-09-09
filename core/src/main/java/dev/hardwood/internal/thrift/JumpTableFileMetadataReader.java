/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.internal.reader.ProjectedColumnMetadata;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;

/// Reads the indexed Parquet footer emitted by the footer benchmark.
///
/// The file remains standard Parquet. Its first `FileMetaData` field points to a footer index
/// whose column-chunk offsets allow projected metadata to be decoded without walking preceding
/// row groups and columns.
public final class JumpTableFileMetadataReader {

    private static final int POINTER_FIELD_ID = 10;
    private static final int POINTER_BYTES = 25;

    private JumpTableFileMetadataReader() {
    }

    /// Returns indexed metadata when the footer starts with a supported jump-table pointer, or
    /// `null` when it is an ordinary standard footer.
    public static FileMetaData tryRead(ByteBuffer footer, long footerStart) {
        Pointer pointer = readPointer(footer);
        if (pointer == null) {
            return null;
        }
        long relativeIndexStart = pointer.indexStart - footerStart;
        requireRange(footer, relativeIndexStart, pointer.indexLength, "footer index");
        Index index = readIndex(slice(footer, relativeIndexStart, pointer.indexLength));
        if (pointer.fileMetadataLength != relativeIndexStart) {
            throw malformed("pointer metadata length does not reach the footer index");
        }

        int version = readI32Field(footer, required(index.fields, 1, "version"));
        List<SchemaElement> schema = readSchema(footer, required(index.fields, 2, "schema"));
        long numRows = readI64Field(footer, required(index.fields, 3, "num_rows"));
        long[] totalByteSizes = new long[index.rowGroups];
        long[] rowCounts = new long[index.rowGroups];
        for (int rowGroup = 0; rowGroup < index.rowGroups; rowGroup++) {
            readRowGroupTail(footer, index.chunkOffset(rowGroup, index.columns),
                    totalByteSizes, rowCounts, rowGroup);
        }
        Map<String, String> keyValues = index.fields.containsKey(5)
                ? readKeyValues(footer, index.fields.get(5))
                : Map.of();
        String createdBy = index.fields.containsKey(6)
                ? readStringField(footer, index.fields.get(6))
                : null;
        List<ColumnOrder> columnOrders = index.fields.containsKey(7)
                ? readColumnOrders(footer, index.fields.get(7))
                : List.of();
        List<FieldPath> paths = FileSchema.fromSchemaElements(schema).getColumns().stream()
                .map(column -> column.fieldPath())
                .toList();
        List<RowGroup> rowGroups = new IndexedRowGroups(
                footer, index, totalByteSizes, rowCounts, paths);
        return new FileMetaData(version, schema, numRows, rowGroups, keyValues, createdBy,
                columnOrders);
    }

    private static Pointer readPointer(ByteBuffer footer) {
        ThriftCompactReader reader = new ThriftCompactReader(footer);
        int header = reader.readFieldHeader();
        if (header == ThriftCompactReader.STOP_FIELD
                || ThriftCompactReader.fieldId(header) != POINTER_FIELD_ID
                || ThriftCompactReader.fieldType(header) != Codes.BINARY) {
            return null;
        }
        byte[] bytes = reader.readBinary();
        if (bytes.length != POINTER_BYTES || bytes[0] != 1) {
            throw malformed("unsupported footer index pointer");
        }
        ByteBuffer pointer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        pointer.get();
        long indexStart = pointer.getLong();
        long indexLength = pointer.getLong();
        long fileMetadataLength = pointer.getLong();
        if (indexStart < 0 || indexLength < 0 || fileMetadataLength < 0) {
            throw malformed("negative footer index pointer value");
        }
        return new Pointer(indexStart, indexLength, fileMetadataLength);
    }

    private static Index readIndex(ByteBuffer bytes) {
        int columns = -1;
        int rowGroups = -1;
        byte[] chunkOffsets = null;
        Map<Integer, Long> fields = Map.of();
        ThriftCompactReader reader = new ThriftCompactReader(bytes);
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int header = reader.readFieldHeader();
                if (header == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                switch (ThriftCompactReader.fieldId(header)) {
                    case 1 -> columns = readI32(reader, header, "num_leaf_columns");
                    case 2 -> rowGroups = readI32(reader, header, "num_row_groups");
                    case 3 -> {
                        if (reader.acceptField(header, Codes.BINARY)) {
                            chunkOffsets = reader.readBinary();
                        }
                    }
                    case 4 -> {
                        if (reader.acceptField(header, Codes.MAP)) {
                            fields = readFieldOffsets(reader);
                        }
                    }
                    default -> reader.skipField(ThriftCompactReader.fieldType(header));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        if (columns <= 0 || rowGroups <= 0 || chunkOffsets == null) {
            throw malformed("footer index is missing required fields");
        }
        int entries = Math.multiplyExact(rowGroups, Math.addExact(columns, 1));
        if (chunkOffsets.length == 0 || chunkOffsets.length % entries != 0) {
            throw malformed("invalid column_chunk_offsets length");
        }
        int width = chunkOffsets.length / entries;
        if (width > Long.BYTES) {
            throw malformed("column_chunk_offsets entries are wider than i64");
        }
        long[] decodedOffsets = new long[entries];
        for (int entry = 0; entry < entries; entry++) {
            int start = Math.multiplyExact(entry, width);
            long value = 0;
            for (int i = 0; i < width; i++) {
                value |= (chunkOffsets[start + i] & 0xFFL) << (i * 8);
            }
            decodedOffsets[entry] = value;
        }
        return new Index(columns, rowGroups, decodedOffsets, fields);
    }

    private static Map<Integer, Long> readFieldOffsets(ThriftCompactReader reader) {
        long countValue = reader.readVarint();
        if (countValue < 0 || countValue > reader.remaining()) {
            throw malformed("invalid field_offsets size");
        }
        int count = Math.toIntExact(countValue);
        if (count == 0) {
            return Map.of();
        }
        byte types = reader.readByte();
        if (((types >>> 4) & 0x0F) != Codes.I16 || (types & 0x0F) != Codes.I64) {
            throw malformed("field_offsets must be map<i16,i64>");
        }
        Map<Integer, Long> fields = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            int field = Math.toIntExact(reader.readZigzag());
            long offset = reader.readZigzag();
            if (offset < 0) {
                throw malformed("negative top-level field offset");
            }
            fields.put(field, offset);
        }
        return Collections.unmodifiableMap(fields);
    }

    private static List<SchemaElement> readSchema(ByteBuffer footer, long offset) {
        ThriftCompactReader reader = fieldReader(footer, offset, (short) 1, Codes.LIST, "schema");
        return reader.readStructList("FileMetaData.schema", SchemaElementReader::read);
    }

    private static Map<String, String> readKeyValues(ByteBuffer footer, long offset) {
        ThriftCompactReader reader = fieldReader(
                footer, offset, (short) 4, Codes.LIST, "key_value_metadata");
        return KeyValueMetadataReader.read(reader, "FileMetaData.key_value_metadata");
    }

    private static String readStringField(ByteBuffer footer, long offset) {
        return fieldReader(footer, offset, (short) 5, Codes.BINARY, "created_by").readString();
    }

    private static List<ColumnOrder> readColumnOrders(ByteBuffer footer, long offset) {
        ThriftCompactReader reader = fieldReader(
                footer, offset, (short) 6, Codes.LIST, "column_orders");
        long list = reader.requireListHeader(Codes.STRUCT, "FileMetaData.column_orders");
        List<ColumnOrder> orders = new ArrayList<>(ThriftCompactReader.listSize(list));
        for (int i = 0; i < ThriftCompactReader.listSize(list); i++) {
            orders.add(ColumnOrderReader.read(reader));
        }
        return Collections.unmodifiableList(orders);
    }

    private static int readI32Field(ByteBuffer footer, long offset) {
        return fieldReader(footer, offset, (short) 0, Codes.I32, "version").readI32();
    }

    private static long readI64Field(ByteBuffer footer, long offset) {
        return fieldReader(footer, offset, (short) 2, Codes.I64, "num_rows").readI64();
    }

    private static ThriftCompactReader fieldReader(ByteBuffer footer, long offset,
            short previousField, byte expectedType, String name) {
        requireRange(footer, offset, 1, name);
        ThriftCompactReader reader = new ThriftCompactReader(footer, Math.toIntExact(offset));
        reader.setFieldIdContext(previousField);
        int header = reader.readFieldHeader();
        if (header == ThriftCompactReader.STOP_FIELD
                || ThriftCompactReader.fieldType(header) != expectedType) {
            throw malformed(name + " has wrong type");
        }
        return reader;
    }

    private static void readRowGroupTail(ByteBuffer footer, long offset, long[] totalByteSizes,
            long[] rowCounts, int rowGroup) {
        requireRange(footer, offset, 1, "row-group tail");
        ThriftCompactReader reader = new ThriftCompactReader(footer, Math.toIntExact(offset));
        reader.setFieldIdContext((short) 1);
        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }
            if (ThriftCompactReader.fieldId(header) == 2
                    && reader.acceptField(header, Codes.I64)) {
                totalByteSizes[rowGroup] = reader.readI64();
            }
            else if (ThriftCompactReader.fieldId(header) == 3
                    && reader.acceptField(header, Codes.I64)) {
                rowCounts[rowGroup] = reader.readI64();
            }
            else {
                reader.skipField(ThriftCompactReader.fieldType(header));
            }
        }
        if (totalByteSizes[rowGroup] < 0 || rowCounts[rowGroup] < 0) {
            throw malformed("negative row-group size or count");
        }
    }

    private static int readI32(ThriftCompactReader reader, int header, String name) {
        if (!reader.acceptField(header, Codes.I32)) {
            throw malformed(name + " has wrong type");
        }
        return reader.readI32();
    }

    private static long required(Map<Integer, Long> fields, int field, String name) {
        Long offset = fields.get(field);
        if (offset == null) {
            throw malformed("missing top-level " + name + " field offset");
        }
        return offset;
    }

    private static ByteBuffer slice(ByteBuffer source, long offset, long length) {
        requireRange(source, offset, length, "slice");
        return source.slice(Math.toIntExact(offset), Math.toIntExact(length));
    }

    private static void requireRange(ByteBuffer buffer, long offset, long length, String name) {
        if (offset < 0 || length < 0 || offset > buffer.limit() || length > buffer.limit() - offset) {
            throw malformed(name + " exceeds footer bounds");
        }
    }

    private static ParquetReadException malformed(String message) {
        return new ParquetReadException("Malformed jump-table footer: " + message);
    }

    private static final class IndexedRowGroups extends AbstractList<RowGroup>
            implements ProjectedColumnMetadata {
        private final ByteBuffer footer;
        private final Index index;
        private final long[] totalByteSizes;
        private final long[] rowCounts;
        private final List<FieldPath> paths;
        private final RowGroup[] cache;

        private IndexedRowGroups(ByteBuffer footer, Index index, long[] totalByteSizes,
                long[] rowCounts, List<FieldPath> paths) {
            this.footer = footer;
            this.index = index;
            this.totalByteSizes = totalByteSizes;
            this.rowCounts = rowCounts;
            this.paths = paths;
            this.cache = new RowGroup[index.rowGroups];
        }

        @Override
        public synchronized RowGroup get(int rowGroup) {
            Objects.checkIndex(rowGroup, cache.length);
            RowGroup existing = cache[rowGroup];
            if (existing != null) {
                return existing;
            }
            RowGroup created = new RowGroup(new IndexedColumns(footer, index, rowGroup, paths),
                    totalByteSizes[rowGroup], rowCounts[rowGroup]);
            cache[rowGroup] = created;
            return created;
        }

        @Override
        public int size() {
            return cache.length;
        }

        @Override
        public void prepareColumns(BitSet columns, List<RowGroup> rowGroups) {
            for (RowGroup rowGroup : rowGroups) {
                List<ColumnChunk> chunks = rowGroup.columns();
                for (int column = columns.nextSetBit(0); column >= 0;
                        column = columns.nextSetBit(column + 1)) {
                    chunks.get(column);
                }
            }
        }
    }

    private static final class IndexedColumns extends AbstractList<ColumnChunk> {
        private final ByteBuffer footer;
        private final Index index;
        private final int rowGroup;
        private final List<FieldPath> paths;
        private final ColumnChunk[] cache;

        private IndexedColumns(ByteBuffer footer, Index index, int rowGroup,
                List<FieldPath> paths) {
            this.footer = footer;
            this.index = index;
            this.rowGroup = rowGroup;
            this.paths = paths;
            this.cache = new ColumnChunk[index.columns];
        }

        @Override
        public synchronized ColumnChunk get(int column) {
            Objects.checkIndex(column, cache.length);
            ColumnChunk existing = cache[column];
            if (existing != null) {
                return existing;
            }
            long from = index.chunkOffset(rowGroup, column);
            long to = index.chunkOffset(rowGroup, column + 1);
            if (to <= from) {
                throw malformed("column chunk offsets are not increasing");
            }
            ColumnChunk created = ColumnChunkReader.read(
                    new ThriftCompactReader(footer, Math.toIntExact(from),
                            Math.toIntExact(to - from)), paths.get(column));
            cache[column] = created;
            return created;
        }

        @Override
        public int size() {
            return cache.length;
        }
    }

    private record Pointer(long indexStart, long indexLength, long fileMetadataLength) {
    }

    private record Index(int columns, int rowGroups, long[] offsets,
                         Map<Integer, Long> fields) {
        private long chunkOffset(int rowGroup, int column) {
            int entry = Math.addExact(Math.multiplyExact(rowGroup, columns + 1), column);
            return offsets[entry];
        }
    }
}
