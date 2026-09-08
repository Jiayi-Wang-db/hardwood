/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;

/// Footer-backed row groups that can prepare query metadata after projection and filter
/// resolution. Ordinary Parquet metadata is already fully decoded and does not implement this.
public interface ProjectedColumnMetadata {

    /// Materializes metadata for the file-schema leaf ordinals this query can touch.
    void prepareColumns(BitSet columns);
}
