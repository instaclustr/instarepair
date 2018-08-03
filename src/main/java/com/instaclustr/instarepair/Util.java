package com.instaclustr.instarepair;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility functions
 */
public class Util {
    /**
     * Split the token range into multiple pieces.
     *
     * @param metadata
     * @param range The token range to split.
     * @param pieces Number of pieces to split the range into.
     * @return List of token ranges.
     */
    public static List<TokenRange> split(Metadata metadata, TokenRange range, int pieces) {
        if (pieces == 1) {
            return unwrap(metadata, range);
        }
        return range.splitEvenly(pieces).stream()
                .filter(r -> !r.getStart().equals(r.getEnd()))
                .flatMap(r -> unwrap(metadata, r).stream())
                .collect(Collectors.toList());
    }

    /**
     * If the token range is wrapped around MAX_TOKEN, then unwrap into (start, MAX_TOKEN] and (MIN_TOKEN, end].
     *
     * @param metadata
     * @param range The token range to unwrap
     * @return The list of unwrapped token ranges.
     */
    public static List<TokenRange> unwrap(Metadata metadata, TokenRange range) {
        if (isWrappedAround(range)) {
            final Token MIN_TOKEN = metadata.newToken(Long.toString(-9223372036854775808L));
            final Token MAX_TOKEN = metadata.newToken(Long.toString(9223372036854775807L));

            // full ring range. Unwrap.
            List<TokenRange> ranges = new ArrayList<>(2);
            ranges.add(metadata.newTokenRange(range.getStart(), MAX_TOKEN));
            if (!range.getEnd().equals(MIN_TOKEN)) {
                ranges.add(metadata.newTokenRange(MIN_TOKEN, range.getEnd()));
            }
            return ranges;
        } else {
            return ImmutableList.of(range);
        }
    }

    /**
     * Check if a token range wraps around the maximum token value. That is from positive to negative token value.
     *
     * @param range Token range to check
     * @return True if the token range is wrapped around.
     */
    public static boolean isWrappedAround(TokenRange range) {
        return range.getStart().equals(range.getEnd()) || range.getStart().compareTo(range.getEnd()) > 0;
    }
}
