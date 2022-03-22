package org.broadinstitute.hellbender.tools.sv;

import htsjdk.samtools.SAMSequenceDictionary;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.codecs.FeatureSink;

import java.util.ArrayList;
import java.util.List;

import static org.broadinstitute.hellbender.tools.sv.DepthEvidence.MISSING_DATA;

/**
 * Merges records for the same interval into a single record, when possible, throws if not possible.
 * It's assumed that all the records refer to the same samples in the same order.  (This can be
 * arranged by calling extractSamples on each record.)
 */
public class DepthEvidenceSortMerger implements FeatureSink<DepthEvidence> {
    private final SAMSequenceDictionary dictionary;
    private final FeatureSink<DepthEvidence> outputSink;
    private final List<DepthEvidence> sameLocusList;
    private DepthEvidence currentLocus;

    public DepthEvidenceSortMerger( final SAMSequenceDictionary dictionary,
                                  final FeatureSink<DepthEvidence> outputSink ) {
        this.dictionary = dictionary;
        this.outputSink = outputSink;
        sameLocusList = new ArrayList<>();
        currentLocus = null;
    }

    @Override
    public void write( final DepthEvidence feature ) {
        if ( currentLocus == null ) {
            currentLocus = feature;
            sameLocusList.add(feature);
        } else {
            int cmp = IntervalUtils.compareLocatables(currentLocus, feature, dictionary);
            if ( cmp == 0 ) {
                sameLocusList.add(feature);
            } else if ( cmp < 0 ) {
                resolveSameLocusFeatures();
                currentLocus = feature;
                sameLocusList.add(feature);
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
        if ( sameLocusList.isEmpty() ) {
            return;
        }
        final DepthEvidence evidence = sameLocusList.get(0);
        final int nEles = sameLocusList.size();
        final int[] counts = evidence.getCounts();
        for ( int ele = 1; ele != nEles; ++ele ) {
            final DepthEvidence tmp = sameLocusList.get(ele);
            final int[] tmpCounts = tmp.getCounts();
            if ( counts.length != tmpCounts.length ) {
                throw new GATKException("All DepthEvidence ought to have the same sample list at this point.");
            }
            for ( int idx = 0; idx != counts.length; ++idx ) {
                final int count = tmpCounts[idx];
                if ( count != MISSING_DATA ) {
                    if ( counts[idx] == MISSING_DATA ) {
                        counts[idx] = tmpCounts[idx];
                    } else {
                        throw new UserException("Multiple sources for count of sample#" + (idx+1) +
                                " at " + evidence.getContig() + ":" + evidence.getStart() + "-" +
                                evidence.getEnd());
                    }
                }
            }
        }
        outputSink.write(evidence);
        sameLocusList.clear();
    }
}
