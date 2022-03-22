package org.broadinstitute.hellbender.tools.sv;

import htsjdk.samtools.SAMSequenceDictionary;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Imposes additional ordering of same-locus LocusDepth records by sample.
 * Imposes uniqueness criterion on <locus, sample>.
 */
public class LocusDepthSortMerger implements FeatureSink<LocusDepth> {
    private final SAMSequenceDictionary dictionary;
    private final FeatureSink<LocusDepth> outputSink;
    private final PriorityQueue<LocusDepth> sameLocusQueue;
    private LocusDepth currentLocus;

    public LocusDepthSortMerger( final SAMSequenceDictionary dictionary,
                                  final FeatureSink<LocusDepth> outputSink ) {
        this.dictionary = dictionary;
        this.outputSink = outputSink;
        sameLocusQueue = new PriorityQueue<>(Comparator.comparing(LocusDepth::getSample));
        currentLocus = null;
    }

    @Override
    public void write( final LocusDepth feature ) {
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

    private void resolveSameLocusFeatures() {
        if ( sameLocusQueue.isEmpty() ) {
            return;
        }
        final Comparator<? super LocusDepth> comparator = sameLocusQueue.comparator();
        LocusDepth lastEvidence = sameLocusQueue.poll();
        while ( !sameLocusQueue.isEmpty() ) {
            final LocusDepth evidence = sameLocusQueue.poll();
            if ( comparator.compare(lastEvidence, evidence) == 0 ) {
                throw new UserException("Two instances of LocusDepth for sample " +
                                        evidence.getSample() + " at " + evidence.getContig() +
                                        ":" + evidence.getStart());
            }
            outputSink.write(lastEvidence);
            lastEvidence = evidence;
        }
        outputSink.write(lastEvidence);
    }
}
