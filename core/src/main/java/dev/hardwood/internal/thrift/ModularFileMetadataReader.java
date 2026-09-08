/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;

/// Experimental adapter from the modular-footer benchmark encoding to Hardwood metadata.
public final class ModularFileMetadataReader {

    private static final int SCHEMA = 0;
    private static final int PLACEMENT = 1;
    private static final int ROW_GROUP_STATISTICS = 2;
    private static final int FILE_METADATA = 5;

    private ModularFileMetadataReader() {
    }

    public static FileMetaData read(InputFile inputFile, long modularStart, long rootOffset,
            long metadataEnd) throws IOException {
        requireRange(modularStart, rootOffset, metadataEnd, "modular root");
        ByteBuffer rootBytes = fetch(inputFile, modularStart + rootOffset,
                Math.toIntExact(metadataEnd - modularStart - rootOffset), "modular-root");
        Root root = readRoot(new ThriftCompactReader(rootBytes));
        Location schemaLocation = required(root.modules, SCHEMA, "schema");
        Location placementLocation = required(root.modules, PLACEMENT, "placement");
        List<SchemaElement> schema = readSchema(module(inputFile, modularStart, metadataEnd,
                schemaLocation, "modular-schema"));
        FileSchema fileSchema = FileSchema.fromSchemaElements(schema);
        if (fileSchema.getColumnCount() != root.numColumns) {
            throw malformed("schema has " + fileSchema.getColumnCount() + " leaf columns, root has "
                    + root.numColumns);
        }
        Placement placement = readPlacement(module(inputFile, modularStart, metadataEnd,
                placementLocation, "modular-placement"), root);
        Statistics[][] statistics = readStatistics(inputFile, modularStart, metadataEnd, root);
        FileDetails details = readFileDetails(inputFile, modularStart, metadataEnd, root);
        List<RowGroup> rowGroups = buildRowGroups(root, fileSchema, placement, statistics);
        return new FileMetaData(root.version, schema, root.numRows, rowGroups,
                details.keyValues, details.createdBy, List.of());
    }

    private static List<RowGroup> buildRowGroups(Root root, FileSchema schema,
            Placement placement, Statistics[][] statistics) {
        List<RowGroup> groups = new ArrayList<>(root.numRowGroups);
        for (int group = 0; group < root.numRowGroups; group++) {
            List<ColumnChunk> chunks = new ArrayList<>(root.numColumns);
            long totalByteSize = 0;
            for (int column = 0; column < root.numColumns; column++) {
                int chunk = column * root.numRowGroups + group;
                Long dictionaryOffset = null;
                long dictionaryIndex = placement.firstDictionaryPages[chunk];
                if (placement.firstDictionaryPages[chunk + 1] != dictionaryIndex) {
                    dictionaryOffset = placement.dictionaryPageOffsets[Math.toIntExact(dictionaryIndex)];
                }
                totalByteSize = Math.addExact(totalByteSize, placement.uncompressedSizes[chunk]);
                FieldPath path = schema.getColumn(column).fieldPath();
                ColumnMetaData metadata = new ColumnMetaData(
                        ThriftEnumLookup.physicalType(Math.toIntExact(placement.physicalTypes[column])),
                        List.of(), path,
                        ThriftEnumLookup.compressionCodec(Math.toIntExact(placement.codecs[chunk])),
                        placement.numValues[chunk], placement.uncompressedSizes[chunk],
                        placement.compressedSizes[chunk], Map.of(), placement.dataPageOffsets[chunk],
                        dictionaryOffset, statistics[column][group], null, null, null, List.of(), null);
                chunks.add(new ColumnChunk(metadata, null, null, null, null, ""));
            }
            groups.add(new RowGroup(Collections.unmodifiableList(chunks), totalByteSize,
                    root.rowGroupNumRows[group]));
        }
        return Collections.unmodifiableList(groups);
    }

    private static Root readRoot(ThriftCompactReader reader) {
        int version = 0;
        int numRowGroups = 0;
        int numColumns = 0;
        long numRows = 0;
        long[] rowCounts = null;
        Map<Integer, Location> modules = new HashMap<>();
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int header = reader.readFieldHeader();
                if (header == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                switch (ThriftCompactReader.fieldId(header)) {
                    case 1 -> version = readI32(reader, header, "version");
                    case 2 -> numRowGroups = readI32(reader, header, "num_row_groups");
                    case 3 -> numColumns = readI32(reader, header, "num_columns");
                    case 4 -> numRows = readI64(reader, header, "num_rows");
                    case 5 -> rowCounts = readI64List(reader, header, "row_group_num_rows");
                    case 6 -> readDirectory(reader, header, modules);
                    default -> reader.skipField(ThriftCompactReader.fieldType(header));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        if (numRowGroups < 0 || numColumns < 0 || numRows < 0 || rowCounts == null
                || rowCounts.length != numRowGroups) {
            throw malformed("invalid root counts");
        }
        return new Root(version, numRowGroups, numColumns, numRows, rowCounts,
                Collections.unmodifiableMap(modules));
    }

    private static void readDirectory(ThriftCompactReader reader, int header,
            Map<Integer, Location> modules) {
        if (!reader.acceptField(header, Codes.LIST)) {
            return;
        }
        long list = reader.requireListHeader(Codes.STRUCT, "ModularFooter.modules");
        for (int i = 0; i < ThriftCompactReader.listSize(list); i++) {
            int kind = -1;
            Location location = null;
            short saved = reader.pushFieldIdContext();
            try {
                while (true) {
                    int field = reader.readFieldHeader();
                    if (field == ThriftCompactReader.STOP_FIELD) {
                        break;
                    }
                    if (ThriftCompactReader.fieldId(field) == 1) {
                        kind = readI32(reader, field, "module kind");
                    }
                    else if (ThriftCompactReader.fieldId(field) == 2
                            && reader.acceptField(field, Codes.STRUCT)) {
                        location = readLocation(reader);
                    }
                    else {
                        reader.skipField(ThriftCompactReader.fieldType(field));
                    }
                }
            }
            finally {
                reader.popFieldIdContext(saved);
            }
            if (kind >= 0 && location != null) {
                modules.put(kind, location);
            }
        }
    }

    private static Location readLocation(ThriftCompactReader reader) {
        long offset = -1;
        long length = -1;
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                if (ThriftCompactReader.fieldId(field) == 1) {
                    offset = readI64(reader, field, "module offset");
                }
                else if (ThriftCompactReader.fieldId(field) == 2) {
                    length = readI64(reader, field, "module length");
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        if (offset < 0 || length < 0) {
            throw malformed("invalid module location");
        }
        return new Location(offset, length);
    }

    private static List<SchemaElement> readSchema(ThriftCompactReader reader) {
        List<SchemaElement> schema = List.of();
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                if (ThriftCompactReader.fieldId(field) == 1 && reader.acceptField(field, Codes.LIST)) {
                    schema = reader.readStructList("SchemaModule.schema", SchemaElementReader::read);
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        return schema;
    }

    private static Placement readPlacement(ThriftCompactReader reader, Root root) {
        long[][] fields = new long[9][];
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                int id = ThriftCompactReader.fieldId(field);
                if (id >= 1 && id <= 9 && reader.acceptField(field, Codes.STRUCT)) {
                    fields[id - 1] = readArray(reader).integers();
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        int chunks = Math.multiplyExact(root.numColumns, root.numRowGroups);
        requireLength(fields[0], chunks, "data_page_offsets");
        requireLength(fields[1], chunks + 1, "first_dictionary_pages");
        requireLength(fields[3], chunks, "compressed_sizes");
        requireLength(fields[4], chunks, "uncompressed_sizes");
        requireLength(fields[5], chunks, "num_values");
        requireLength(fields[6], chunks, "codecs");
        requireLength(fields[7], root.numColumns, "physical_types");
        return new Placement(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5],
                fields[6], fields[7]);
    }

    private static Statistics[][] readStatistics(InputFile inputFile, long modularStart,
            long metadataEnd, Root root) throws IOException {
        Statistics[][] result = new Statistics[root.numColumns][root.numRowGroups];
        Location location = root.modules.get(ROW_GROUP_STATISTICS);
        if (location == null) {
            return result;
        }
        ThriftCompactReader directory = module(inputFile, modularStart, metadataEnd, location,
                "modular-statistics-directory");
        long[] offsets = null;
        short saved = directory.pushFieldIdContext();
        try {
            while (true) {
                int field = directory.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                if (ThriftCompactReader.fieldId(field) == 1
                        && directory.acceptField(field, Codes.STRUCT)) {
                    offsets = readArray(directory).integers();
                }
                else {
                    directory.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            directory.popFieldIdContext(saved);
        }
        requireLength(offsets, root.numColumns + 1, "column_offsets");
        for (int column = 0; column < root.numColumns; column++) {
            long length = offsets[column + 1] - offsets[column];
            if (length > 0) {
                requireRange(modularStart, offsets[column], metadataEnd,
                        "column statistics offset");
                ByteBuffer bytes = fetch(inputFile, modularStart + offsets[column],
                        Math.toIntExact(length), "modular-column-statistics");
                readColumnStatistics(new ThriftCompactReader(bytes), result[column]);
            }
        }
        return result;
    }

    private static void readColumnStatistics(ThriftCompactReader reader, Statistics[] output) {
        ArrayData nulls = null;
        ArrayData prefixes = null;
        ArrayData mins = null;
        ArrayData maxs = null;
        ArrayData minExact = null;
        ArrayData maxExact = null;
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                int id = ThriftCompactReader.fieldId(field);
                if (id >= 1 && id <= 6 && reader.acceptField(field, Codes.STRUCT)) {
                    ArrayData value = id >= 2 && id <= 4
                            ? readByteArray(reader)
                            : readArray(reader);
                    switch (id) {
                        case 1 -> nulls = value;
                        case 2 -> prefixes = value;
                        case 3 -> mins = value;
                        case 4 -> maxs = value;
                        case 5 -> minExact = value;
                        case 6 -> maxExact = value;
                        default -> throw new AssertionError();
                    }
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        for (int group = 0; group < output.length; group++) {
            byte[] prefix = bytesAt(prefixes, group);
            byte[] minSuffix = bytesAt(mins, group);
            byte[] maxSuffix = bytesAt(maxs, group);
            byte[] min = concat(prefix, minSuffix);
            byte[] max = concat(prefix, maxSuffix);
            Long nullCount = integerAt(nulls, group);
            if (min != null || max != null || nullCount != null) {
                output[group] = new Statistics(min, max, nullCount, null, false,
                        booleanAt(minExact, group, true), booleanAt(maxExact, group, true), null);
            }
        }
    }

    private static FileDetails readFileDetails(InputFile inputFile, long modularStart,
            long metadataEnd, Root root) throws IOException {
        Location location = root.modules.get(FILE_METADATA);
        if (location == null) {
            return new FileDetails(null, Map.of());
        }
        ThriftCompactReader reader = module(inputFile, modularStart, metadataEnd, location,
                "modular-file-metadata");
        String createdBy = null;
        Map<String, String> keyValues = Map.of();
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                if (ThriftCompactReader.fieldId(field) == 1
                        && reader.acceptField(field, Codes.BINARY)) {
                    createdBy = reader.readString();
                }
                else if (ThriftCompactReader.fieldId(field) == 2
                        && reader.acceptField(field, Codes.LIST)) {
                    keyValues = KeyValueMetadataReader.read(reader,
                            "FileMetadataModule.key_value_metadata");
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        return new FileDetails(createdBy, keyValues);
    }

    private static ArrayData readArray(ThriftCompactReader reader) {
        return readArrayPage(reader, false);
    }

    private static ArrayData readByteArray(ThriftCompactReader reader) {
        return readArrayPage(reader, true);
    }

    private static ArrayData readArrayPage(ThriftCompactReader reader, boolean byteValues) {
        byte[] data = null;
        int encoding = -1;
        int count = -1;
        Parameters parameters = null;
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                switch (ThriftCompactReader.fieldId(field)) {
                    case 1 -> {
                        if (reader.acceptField(field, Codes.BINARY)) {
                            data = reader.readBinary();
                        }
                    }
                    case 2 -> encoding = readI32(reader, field, "ArrayPage.encoding");
                    case 3 -> count = readI32(reader, field, "ArrayPage.num_values");
                    case 4 -> {
                        if (reader.acceptField(field, Codes.STRUCT)) {
                            parameters = readParameters(reader);
                        }
                    }
                    default -> reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        if (data == null || count < 0 || parameters == null || encoding != parameters.encoding) {
            throw malformed("invalid ArrayPage");
        }
        return byteValues
                ? ArrayData.decodeBytes(data, count, parameters)
                : ArrayData.decodeIntegers(data, count, parameters);
    }

    private static Parameters readParameters(ThriftCompactReader reader) {
        Parameters result = null;
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                int encoding = ThriftCompactReader.fieldId(field) - 1;
                if ((encoding == 0 || encoding == 1) && reader.acceptField(field, Codes.STRUCT)) {
                    result = readParameterBody(reader, encoding);
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        return result;
    }

    private static Parameters readParameterBody(ThriftCompactReader reader, int encoding) {
        int present = -1;
        int positionWidth = 0;
        int valueWidth = -1;
        short saved = reader.pushFieldIdContext();
        try {
            while (true) {
                int field = reader.readFieldHeader();
                if (field == ThriftCompactReader.STOP_FIELD) {
                    break;
                }
                int id = ThriftCompactReader.fieldId(field);
                if (encoding == 0 && id == 1 && reader.acceptField(field, Codes.BYTE)) {
                    valueWidth = reader.readByte() & 0xFF;
                }
                else if (encoding == 0 && id == 2) {
                    present = readI32(reader, field, "BitsetParameters.num_present");
                }
                else if (encoding == 1 && id == 1) {
                    present = readI32(reader, field, "PresentIndexParameters.num_present");
                }
                else if (encoding == 1 && id == 2 && reader.acceptField(field, Codes.BYTE)) {
                    positionWidth = reader.readByte() & 0xFF;
                }
                else if (encoding == 1 && id == 3 && reader.acceptField(field, Codes.BYTE)) {
                    valueWidth = reader.readByte() & 0xFF;
                }
                else {
                    reader.skipField(ThriftCompactReader.fieldType(field));
                }
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
        if (present < 0 || valueWidth < 0 || valueWidth > 64 || positionWidth > 64) {
            throw malformed("invalid ArrayPage parameters");
        }
        return new Parameters(encoding, present, positionWidth, valueWidth);
    }

    private static int readI32(ThriftCompactReader reader, int header, String name) {
        if (!reader.acceptField(header, Codes.I32)) {
            throw malformed(name + " has wrong type");
        }
        return reader.readI32();
    }

    private static long readI64(ThriftCompactReader reader, int header, String name) {
        if (!reader.acceptField(header, Codes.I64)) {
            throw malformed(name + " has wrong type");
        }
        return reader.readI64();
    }

    private static long[] readI64List(ThriftCompactReader reader, int header, String name) {
        if (!reader.acceptField(header, Codes.LIST)) {
            throw malformed(name + " has wrong type");
        }
        long list = reader.requireListHeader(Codes.I64, name);
        long[] values = new long[ThriftCompactReader.listSize(list)];
        for (int i = 0; i < values.length; i++) {
            values[i] = reader.readI64();
        }
        return values;
    }

    private static ThriftCompactReader module(InputFile inputFile, long modularStart,
            long metadataEnd, Location location, String reason) throws IOException {
        requireRange(modularStart, location.offset, metadataEnd, reason);
        if (location.length > metadataEnd - modularStart - location.offset) {
            throw malformed(reason + " exceeds metadata bounds");
        }
        return new ThriftCompactReader(fetch(inputFile, modularStart + location.offset,
                Math.toIntExact(location.length), reason));
    }

    private static ByteBuffer fetch(InputFile inputFile, long offset, int length, String reason)
            throws IOException {
        try (FetchReason.Scope ignored = FetchReason.set(reason)) {
            return inputFile.readRange(offset, length);
        }
    }

    private static void requireRange(long start, long offset, long end, String name) {
        if (start < 4 || offset < 0 || start > end || offset > end - start) {
            throw malformed("invalid " + name + " range");
        }
    }

    private static Location required(Map<Integer, Location> modules, int kind, String name) {
        Location location = modules.get(kind);
        if (location == null) {
            throw malformed("missing required " + name + " module");
        }
        return location;
    }

    private static void requireLength(long[] values, int expected, String name) {
        if (values == null || values.length != expected) {
            throw malformed(name + " must contain " + expected + " values");
        }
    }

    private static Long integerAt(ArrayData data, int index) {
        return data == null || !data.present[index] ? null : data.integers[index];
    }

    private static byte[] bytesAt(ArrayData data, int index) {
        return data == null || !data.present[index] ? null : data.bytes[index];
    }

    private static boolean booleanAt(ArrayData data, int index, boolean fallback) {
        Long value = integerAt(data, index);
        return value == null ? fallback : value != 0;
    }

    private static byte[] concat(byte[] prefix, byte[] suffix) {
        if (prefix == null || suffix == null) {
            return null;
        }
        byte[] result = new byte[prefix.length + suffix.length];
        System.arraycopy(prefix, 0, result, 0, prefix.length);
        System.arraycopy(suffix, 0, result, prefix.length, suffix.length);
        return result;
    }

    private static ParquetReadException malformed(String message) {
        return new ParquetReadException("Malformed modular footer: " + message);
    }

    private record Root(int version, int numRowGroups, int numColumns, long numRows,
                        long[] rowGroupNumRows, Map<Integer, Location> modules) {
    }

    private record Location(long offset, long length) {
    }

    private record Placement(long[] dataPageOffsets, long[] firstDictionaryPages,
                             long[] dictionaryPageOffsets, long[] compressedSizes,
                             long[] uncompressedSizes, long[] numValues, long[] codecs,
                             long[] physicalTypes) {
    }

    private record FileDetails(String createdBy, Map<String, String> keyValues) {
    }

    private record Parameters(int encoding, int present, int positionWidth, int valueWidth) {
    }

    private record ArrayData(long[] integers, byte[][] bytes, boolean[] present) {
        private static ArrayData decodeIntegers(byte[] data, int count, Parameters parameters) {
            boolean[] present = new boolean[count];
            long[] integers = new long[count];
            int bit = 0;
            int[] positions = new int[parameters.present];
            if (parameters.encoding == 0) {
                if (parameters.present != count) {
                    for (int i = 0; i < count; i++) {
                        present[i] = (data[i >>> 3] & (1 << (i & 7))) != 0;
                    }
                    bit = ((count + 7) / 8) * 8;
                }
                else {
                    java.util.Arrays.fill(present, true);
                }
                for (int i = 0; i < count; i++) {
                    integers[i] = unpack(data, bit, parameters.valueWidth);
                    bit += parameters.valueWidth;
                }
            }
            else {
                for (int i = 0; i < positions.length; i++) {
                    positions[i] = Math.toIntExact(unpack(data, bit, parameters.positionWidth));
                    bit += parameters.positionWidth;
                    if (positions[i] < 0 || positions[i] >= count) {
                        throw malformed("array position is outside its logical domain");
                    }
                    present[positions[i]] = true;
                }
                int valueStart = roundToByte(bit);
                for (int i = 0; i < positions.length; i++) {
                    integers[positions[i]] = unpack(data, valueStart, parameters.valueWidth);
                    valueStart += parameters.valueWidth;
                }
            }
            return new ArrayData(integers, null, present);
        }

        private static ArrayData decodeBytes(byte[] data, int count, Parameters parameters) {
            if (parameters.encoding != 1) {
                throw malformed("byte arrays require PRESENT_INDEX encoding");
            }
            boolean[] present = new boolean[count];
            int[] positions = new int[parameters.present];
            int bit = 0;
            for (int i = 0; i < positions.length; i++) {
                positions[i] = Math.toIntExact(unpack(data, bit, parameters.positionWidth));
                bit += parameters.positionWidth;
                if (positions[i] < 0 || positions[i] >= count) {
                    throw malformed("array position is outside its logical domain");
                }
                present[positions[i]] = true;
            }
            bit = roundToByte(bit);
            long[] offsets = new long[positions.length + 1];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = unpack(data, bit, parameters.valueWidth);
                bit += parameters.valueWidth;
            }
            int payloadStart = roundToByte(bit) / 8;
            byte[][] values = new byte[count][];
            for (int i = 0; i < positions.length; i++) {
                int from = Math.toIntExact(offsets[i]);
                int to = Math.toIntExact(offsets[i + 1]);
                if (from < 0 || to < from || payloadStart + to > data.length) {
                    throw malformed("invalid byte-array offsets");
                }
                values[positions[i]] = java.util.Arrays.copyOfRange(
                        data, payloadStart + from, payloadStart + to);
            }
            return new ArrayData(null, values, present);
        }

        private static int roundToByte(int bitOffset) {
            return Math.multiplyExact(Math.floorDiv(Math.addExact(bitOffset, 7), 8), 8);
        }

        private static long unpack(byte[] data, int bitOffset, int width) {
            if (bitOffset < 0 || width < 0 || bitOffset > data.length * 8 - width) {
                throw malformed("bit-packed array exceeds its payload");
            }
            long value = 0;
            for (int bit = 0; bit < width; bit++) {
                int source = bitOffset + bit;
                if ((data[source >>> 3] & (1 << (source & 7))) != 0) {
                    value |= 1L << bit;
                }
            }
            return value;
        }
    }
}
