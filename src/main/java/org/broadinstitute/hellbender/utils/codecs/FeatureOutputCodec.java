package org.broadinstitute.hellbender.utils.codecs;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.tribble.Feature;
import org.broadinstitute.hellbender.engine.GATKPath;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public interface FeatureOutputCodec<F extends Feature, S extends FeatureSink<F>> {
    boolean canDecode( String path );
    Class<F> getFeatureType();
    S makeSink( GATKPath path, SAMSequenceDictionary dict, List<String> sampleNames, int compressionLevel );
    void encode( F feature, S sink ) throws IOException;
    Comparator<F> getSameLocusComparator();
    void resolveSameLocusFeatures( PriorityQueue<F> queue, S sink );
}
