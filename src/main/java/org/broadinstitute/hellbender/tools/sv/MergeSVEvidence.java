package org.broadinstitute.hellbender.tools.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.tribble.Feature;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.argparser.ExperimentalFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureInput;
import org.broadinstitute.hellbender.engine.FeatureMergingWalker;
import org.broadinstitute.hellbender.engine.GATKPath;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.codecs.*;

import java.util.ArrayList;
import java.util.List;

@CommandLineProgramProperties(
        summary = "Merges SV evidence records",
        oneLineSummary = "Merges SV evidence records",
        programGroup = StructuralVariantDiscoveryProgramGroup.class
)
@ExperimentalFeature
public class MergeSVEvidence extends FeatureMergingWalker<SVFeature> {
    public static final String EVIDENCE_FILE_NAME = "evidence-file";
    public static final String SAMPLE_NAMES = "sample-names";
    public static final String COMPRESSION_LEVEL_NAME = "compression-level";

    @Argument(
            doc = "Input feature file URI(s) with extension '"
                    + SplitReadEvidenceCodec.FORMAT_SUFFIX + "', '"
                    + DiscordantPairEvidenceCodec.FORMAT_SUFFIX + "', '"
                    + LocusDepthCodec.FORMAT_SUFFIX + "', '"
                    + BafEvidenceCodec.FORMAT_SUFFIX + "', or '"
                    + DepthEvidenceCodec.FORMAT_SUFFIX + "' (may be gzipped). "
                    + "Can also handle bci rather than txt files.",
            fullName = EVIDENCE_FILE_NAME,
            shortName = StandardArgumentDefinitions.FEATURE_SHORT_NAME
    )
    private List<FeatureInput<Feature>> inputPaths;

    @Argument(doc = "List of sample names to extract from the sources.  If not specified, " +
                "all samples will be merged.",
              fullName = SAMPLE_NAMES, optional = true)
    @VisibleForTesting
    List<String> sampleNames;

    @Argument(
            doc = "Output file for features of a type matching the input. Will be indexed if it " +
                    "has a block-compressed extension (e.g. '.gz' or '.bci').",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME
    )
    private GATKPath outputFilePath;

    @Argument(
            doc = "Output compression level",
            fullName = COMPRESSION_LEVEL_NAME,
            minValue = 0, maxValue = 9, optional = true
    )
    private int compressionLevel = 4;

    private FeatureSink<Feature> outputSink;

    @Override
    @SuppressWarnings("unchecked")
    public void onTraversalStart() {
        super.onTraversalStart();
        final FeatureOutputCodec<? extends Feature, ? extends FeatureSink<? extends Feature>> codec =
                FeatureOutputCodecFinder.find(outputFilePath);
        final Class<? extends Feature> outputClass = codec.getFeatureType();
        for ( FeatureInput<? extends Feature> input : inputPaths ) {
            try {
                final Class<? extends Feature> inputClass =
                        input.getFeatureCodecClass().getDeclaredConstructor().newInstance().getFeatureType();
                if ( !outputClass.isAssignableFrom(inputClass) ) {
                    throw new UserException("Incompatible feature input " + input.getFeaturePath() +
                            " produces features of type " + inputClass.getSimpleName() +
                            " rather than features of type " + outputClass.getSimpleName() +
                            " as dictated by the output path " + outputFilePath);
                }
            } catch ( final ReflectiveOperationException roe ) {
                throw new GATKException("Failed to instantiate codec " +
                                            input.getFeatureCodecClass().getSimpleName());
            }
        }
        if ( sampleNames == null || sampleNames.isEmpty() ) {
            sampleNames = new ArrayList<>(getSampleNames());
        }
        outputSink = (FeatureSink<Feature>)codec.makeSink(outputFilePath, getDictionary(),
                                                            sampleNames, compressionLevel);
    }

    @Override
    public void apply( final SVFeature featureArg, final Object header ) {
        final SVFeature feature = featureArg.extractSamples(sampleNames, header);
        if ( feature != null ) {
            outputSink.write(feature);
        }
    }

    @Override
    public Object onTraversalSuccess() {
        super.onTraversalSuccess();
        outputSink.close();
        return null;
    }
}
