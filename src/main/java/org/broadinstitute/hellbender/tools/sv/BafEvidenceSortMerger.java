package org.broadinstitute.hellbender.tools.sv;

import htsjdk.samtools.SAMSequenceDictionary;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Imposes additional ordering of same-locus BafEvidence by sample.
 * Imposes uniqueness criterion on <locus, sample>.
 */
public class BafEvidenceSortMerger implements FeatureSink<BafEvidence> {
    private final SAMSequenceDictionary dictionary;
    private final FeatureSink<BafEvidence> outputSink;
    private final PriorityQueue<BafEvidence> sameLocusQueue;
    private BafEvidence currentLocus;

    public BafEvidenceSortMerger( final SAMSequenceDictionary dictionary,
                                  final FeatureSink<BafEvidence> outputSink ) {
        this.dictionary = dictionary;
        this.outputSink = outputSink;
        sameLocusQueue = new PriorityQueue<>(Comparator.comparing(BafEvidence::getSample));
        currentLocus = null;
    }

    @Override
    public void write( final BafEvidence feature ) {
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
        final Comparator<? super BafEvidence> comparator = sameLocusQueue.comparator();
        BafEvidence lastEvidence = sameLocusQueue.poll();
        while ( !sameLocusQueue.isEmpty() ) {
            final BafEvidence evidence = sameLocusQueue.poll();
            if ( comparator.compare(lastEvidence, evidence) == 0 ) {
                throw new UserException("Two instances of BafEvidence for sample " +
                                        evidence.getSample() + " at " + evidence.getContig() +
                                        ":" + evidence.getStart());
            }
            outputSink.write(lastEvidence);
            lastEvidence = evidence;
        }
        outputSink.write(lastEvidence);
    }
}
