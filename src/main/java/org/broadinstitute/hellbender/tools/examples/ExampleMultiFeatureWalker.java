package org.broadinstitute.hellbender.tools.examples;

import htsjdk.tribble.Feature;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.FeatureInput;
import org.broadinstitute.hellbender.engine.MultiFeatureWalker;
import org.broadinstitute.hellbender.engine.ReadsContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;

import java.util.ArrayList;
import java.util.List;

public class ExampleMultiFeatureWalker extends MultiFeatureWalker<Feature> {
    @Argument( fullName = StandardArgumentDefinitions.FEATURE_LONG_NAME,
            shortName = StandardArgumentDefinitions.FEATURE_SHORT_NAME )
    private List<FeatureInput<Feature>> featureInputs;

    // We'll just keep track of the Features we see, in the order that we see them.
    final List<Feature> features = new ArrayList<>();

    @Override public void apply( final Feature feature,
                                 final Object header,
                                 final ReadsContext readsContext,
                                 final ReferenceContext referenceContext ) {
        // Track observed features, just for fun.
        features.add(feature);
    }
}
