package org.broadinstitute.hellbender.tools.sv;

import htsjdk.samtools.SAMSequenceDictionary;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Imposes additional ordering of same-locus SplitReadEvidence by sample and strand.
 * Imposes uniqueness criterion on <locus, sample, strand>.
 */
public class SplitReadEvidenceSortMerger implements FeatureSink<SplitReadEvidence> {
    private final SAMSequenceDictionary dictionary;
    private final FeatureSink<SplitReadEvidence> outputSink;
    private final PriorityQueue<SplitReadEvidence> sameLocusQueue;
    private SplitReadEvidence currentLocus;

    public SplitReadEvidenceSortMerger( final SAMSequenceDictionary dictionary,
                                          final FeatureSink<SplitReadEvidence> outputSink ) {
        this.dictionary = dictionary;
        this.outputSink = outputSink;
        final Comparator<SplitReadEvidence> comparator =
                Comparator.comparing(SplitReadEvidence::getSample).thenComparing(
                        (f1, f2) -> f1.getStrand() == f2.getStrand() ? 0 : f1.getStrand() ? 1 : -1);
        sameLocusQueue = new PriorityQueue<>(comparator);
        currentLocus = null;
    }

    @Override
    public void write( final SplitReadEvidence feature ) {
        if ( currentLocus == null ) {
            currentLocus = feature;
            sameLocusQueue.add(feature);
        } else {
            int cmp = IntervalUtils.compareLocatables(currentLocus, feature, dictionary);
            if ( cmp == 0 ) {
                sameLocusQueue.add(feature);
            } else if ( cmp < 0 ) {
                resolveSameLocusFeatures();
                currentLocus = feature;
                sameLocusQueue.add(feature);
            } else {
                throw new GATKException("features not presented in dictionary order");
            }
        }
    }

    @Override
    public void close() {
        resolveSameLocusFeatures();
        outputSink.close();
    }

    public void resolveSameLocusFeatures() {
        if ( sameLocusQueue.isEmpty() ) {
            return;
        }
        final Comparator<? super SplitReadEvidence> comparator = sameLocusQueue.comparator();
        SplitReadEvidence lastEvidence = sameLocusQueue.poll();
        while ( !sameLocusQueue.isEmpty() ) {
            final SplitReadEvidence evidence = sameLocusQueue.poll();
            if ( comparator.compare(lastEvidence, evidence) == 0 ) {
                throw new UserException("Two instances of SplitReadEvidence for sample " +
                        evidence.getSample() + " at " + evidence.getContig() + ":" +
                        evidence.getStart() + (evidence.getStrand() ? " right" : " left"));
            }
            outputSink.write(lastEvidence);
            lastEvidence = evidence;
        }
        outputSink.write(lastEvidence);
    }
}
