package org.broadinstitute.hellbender.tools.sv;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

public final class BafEvidence implements SVFeature {
    final String sample;
    final String contig;
    final int position;
    final double value;

    public final static String BCI_VERSION = "1.0";
    public static final Comparator<BafEvidence> comparator =
            Comparator.comparing(BafEvidence::getSample);

    public BafEvidence( final String sample, final String contig,
                        final int position, final double value ) {
        Utils.nonNull(sample);
        Utils.nonNull(contig);
        this.sample = sample;
        this.contig = contig;
        this.position = position;
        this.value = value;
    }

    public String getSample() {
        return sample;
    }

    @Override
    public String getContig() {
        return contig;
    }

    @Override
    public int getStart() {
        return position;
    }

    @Override
    public int getEnd() {
        return position;
    }

    public double getValue() {
        return value;
    }

    @Override
    public BafEvidence extractSamples( final Set<String> sampleList, final Object header ) {
        return sampleList.contains(sample) ? this : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BafEvidence)) return false;
        BafEvidence that = (BafEvidence) o;
        return position == that.position &&
                Double.compare(that.value, value) == 0 &&
                sample.equals(that.sample) &&
                contig.equals(that.contig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sample, contig, position, value);
    }

    public static void resolveSameLocusFeatures( final PriorityQueue<BafEvidence> queue,
                                                 final FeatureSink<BafEvidence> sink ) {
        if ( queue.isEmpty() ) {
            return;
        }
        BafEvidence lastEvidence = queue.poll();
        while ( !queue.isEmpty() ) {
            final BafEvidence evidence = queue.poll();
            if ( comparator.compare(lastEvidence, evidence) == 0 ) {
                throw new UserException("Two instances of BafEvidence for sample " +
                        evidence.sample + " at " + evidence.contig + ":" + evidence.position);
            }
            sink.write(lastEvidence);
            lastEvidence = evidence;
        }
        sink.write(lastEvidence);
    }
}
