package org.broadinstitute.hellbender.tools.sv;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

public final class SplitReadEvidence implements SVFeature {

    final String sample;
    final String contig;
    final int position;
    final int count;
    final boolean strand;

    public final static String BCI_VERSION = "1.0";
    public final static Comparator<SplitReadEvidence> comparator =
            Comparator.comparing(SplitReadEvidence::getSample)
                    .thenComparing((f1, f2) -> f1.strand == f2.strand ? 0 : f1.strand ? 1 : -1);

    public SplitReadEvidence( final String sample, final String contig, final int position,
                              final int count, final boolean strand ) {
        Utils.nonNull(sample);
        Utils.nonNull(contig);
        this.sample = sample;
        this.contig = contig;
        this.position = position;
        this.count = count;
        this.strand = strand;
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

    public boolean getStrand() {
        return strand;
    }

    public int getCount() {
        return count;
    }

    @Override
    public SplitReadEvidence extractSamples( final Set<String> sampleNames, final Object header ) {
        return sampleNames.contains(sample) ? this : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitReadEvidence)) return false;
        SplitReadEvidence that = (SplitReadEvidence) o;
        return position == that.position &&
                count == that.count &&
                strand == that.strand &&
                sample.equals(that.sample) &&
                contig.equals(that.contig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sample, contig, position, count, strand);
    }

    public static void resolveSameLocusFeatures( final PriorityQueue<SplitReadEvidence> queue,
                                                 final FeatureSink<SplitReadEvidence> sink ) {
        if ( queue.isEmpty() ) {
            return;
        }
        SplitReadEvidence lastEvidence = queue.poll();
        while ( !queue.isEmpty() ) {
            final SplitReadEvidence evidence = queue.poll();
            if ( comparator.compare(lastEvidence, evidence) == 0 ) {
                throw new UserException("Two instances of SplitReadEvidence for sample " +
                        evidence.sample + " at " + evidence.contig + ":" + evidence.position +
                        (evidence.strand ? " right" : " left"));
            }
            sink.write(lastEvidence);
            lastEvidence = evidence;
        }
        sink.write(lastEvidence);
    }
}