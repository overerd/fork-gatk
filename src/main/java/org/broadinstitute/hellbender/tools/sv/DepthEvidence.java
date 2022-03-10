package org.broadinstitute.hellbender.tools.sv;

import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.*;

public final class DepthEvidence implements SVFeature {

    final String contig;
    final int start;
    final int end;
    final int[] counts;

    public static final String BCI_VERSION = "1.0";
    public static final int MISSING_DATA = -1;
    public static final Comparator<DepthEvidence> comparator = (f1, f2) -> 0;

    public DepthEvidence(final String contig, int start, final int end, final int[] counts) {
        Utils.nonNull(contig);
        Utils.nonNull(counts);
        this.contig = contig;
        this.start = start;
        this.end = end;
        this.counts = counts;
    }

    @Override
    public String getContig() {
        return contig;
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    public int getEnd() {
        return end;
    }

    public int[] getCounts() { return counts; }

    @Override
    public DepthEvidence extractSamples( final Set<String> sampleNames, final Object headerObj ) {
        if ( !(headerObj instanceof SVFeaturesHeader) ) {
            throw new UserException("DepthEvidence feature source without a header.  " +
                                    "We don't know which samples we have.");
        }
        final SVFeaturesHeader header = (SVFeaturesHeader)headerObj;
        final int nCounts = sampleNames.size();
        final int[] newCounts = new int[nCounts];
        int idx = 0;
        for ( final String sampleName : sampleNames ) {
            final Integer sampleIndex = header.getSampleIndex(sampleName);
            newCounts[idx++] = sampleIndex == null ? MISSING_DATA : counts[sampleIndex];
        }
        return new DepthEvidence(contig, start, end, newCounts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DepthEvidence)) return false;
        DepthEvidence that = (DepthEvidence) o;
        return start == that.start &&
                end == that.end &&
                contig.equals(that.contig) &&
                Arrays.equals(counts, that.counts);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(contig, start, end);
        result = 31 * result + Arrays.hashCode(counts);
        return result;
    }

    public static void resolveSameLocusFeatures( final PriorityQueue<DepthEvidence> queue,
                                                 final FeatureSink<DepthEvidence> sink ) {
        if ( queue.isEmpty() ) {
            return;
        }
        final DepthEvidence evidence = queue.poll();
        final int[] evCounts = evidence.counts;
        while ( !queue.isEmpty() ) {
            final DepthEvidence tmp = queue.poll();
            final int[] tmpCounts = tmp.counts;
            if ( evCounts.length != tmpCounts.length ) {
                throw new GATKException("All DepthEvidence ought to have the same sample list at this point.");
            }
            for ( int idx = 0; idx != evCounts.length; ++idx ) {
                final int count = tmpCounts[idx];
                if ( count != MISSING_DATA ) {
                    if ( evCounts[idx] == MISSING_DATA ) {
                        evCounts[idx] = tmpCounts[idx];
                    } else {
                        throw new UserException("Multiple sources for count of sample#" + (idx+1) +
                                " at " + evidence.getContig() + ":" + evidence.getStart() + "-" +
                                evidence.getEnd());
                    }
                }
            }
        }
        sink.write(evidence);
    }
}
