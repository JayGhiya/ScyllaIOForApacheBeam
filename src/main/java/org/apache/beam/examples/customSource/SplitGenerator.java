package org.apache.beam.examples.customSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/** Splits given Cassandra table's token range into splits. */
final class SplitGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SplitGenerator.class);

    private final String partitioner;
    private final BigInteger rangeMin;
    private final BigInteger rangeMax;
    private final BigInteger rangeSize;

    SplitGenerator(String partitioner) {
        rangeMin = getRangeMin(partitioner);
        rangeMax = getRangeMax(partitioner);
        rangeSize = getRangeSize(partitioner);
        this.partitioner = partitioner;
    }

    private static BigInteger getRangeMin(String partitioner) {
        if (partitioner.endsWith("RandomPartitioner")) {
            return BigInteger.ZERO;
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            return new BigInteger("2").pow(63).negate();
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported partitioner. " + "Only Random and Murmur3 are supported");
        }
    }

    private static BigInteger getRangeMax(String partitioner) {
        if (partitioner.endsWith("RandomPartitioner")) {
            return new BigInteger("2").pow(127).subtract(BigInteger.ONE);
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            return new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported partitioner. " + "Only Random and Murmur3 are supported");
        }
    }

    static BigInteger getRangeSize(String partitioner) {
        return getRangeMax(partitioner).subtract(getRangeMin(partitioner)).add(BigInteger.ONE);
    }

}
