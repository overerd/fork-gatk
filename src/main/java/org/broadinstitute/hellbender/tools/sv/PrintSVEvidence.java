package org.broadinstitute.hellbender.tools.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.tribble.Feature;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.argparser.ExperimentalFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureInput;
import org.broadinstitute.hellbender.engine.MultiFeatureWalker;
import org.broadinstitute.hellbender.engine.GATKPath;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.codecs.*;

import java.util.*;

@CommandLineProgramProperties(
        summary = "Merges SV evidence records",
        oneLineSummary = "Merges SV evidence records",
        programGroup = StructuralVariantDiscoveryProgramGroup.class
)
@ExperimentalFeature
public class PrintSVEvidence extends MultiFeatureWalker<SVFeature> {
    public static final String EVIDENCE_FILE_NAME = "evidence-file";
    public static final String SAMPLE_NAMES_NAME = "sample-names";
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
    private List<FeatureInput<SVFeature>> inputPaths;

    @Argument(doc = "List of sample names to extract from the sources (either as a .list file or " +
            " as repeated arguments).  If not specified, all samples will be merged.",
              fullName = SAMPLE_NAMES_NAME, optional = true)
    @VisibleForTesting
    Set<String> sampleNames = new LinkedHashSet<>();

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

    private FeatureOutputCodec<SVFeature, FeatureSink<SVFeature>> outputCodec;
    private FeatureSink<SVFeature> outputSink;
    private boolean noSampleFiltering = false;
    private SVFeature currentLocus;
    private PriorityQueue<SVFeature> sameLocusQueue;

    @Override
    @SuppressWarnings("unchecked")
    public void onTraversalStart() {
        super.onTraversalStart();

        final FeatureOutputCodec<? extends Feature, ? extends FeatureSink<? extends Feature>> codec =
                FeatureOutputCodecFinder.find(outputFilePath);
        final Class<? extends Feature> outputClass = codec.getFeatureType();
        if ( !SVFeature.class.isAssignableFrom(outputClass) ) {
            throw new UserException("Output file " + outputFilePath + " implies Feature subtype " +
                    outputClass.getSimpleName() + " but this tool requires an SVFeature subtype.");
        }

        for ( FeatureInput<SVFeature> input : inputPaths ) {
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
        if ( sampleNames.isEmpty() ) {
            // use the complete set of sample names we found in the headers of the feature files
            sampleNames.addAll(getSampleNames());
            if ( sampleNames.isEmpty() ) {
                noSampleFiltering = true;
            }
        }
        // the validity of these casts were checked at the beginning of this method
        outputCodec = (FeatureOutputCodec<SVFeature, FeatureSink<SVFeature>>)codec;
        outputSink = (FeatureSink<SVFeature>)codec.makeSink(outputFilePath, getDictionary(),
                                                    new ArrayList<>(sampleNames), compressionLevel);
        sameLocusQueue = new PriorityQueue<>(Math.max(30,sampleNames.size()),
                                            (Comparator<SVFeature>)codec.getSameLocusComparator());
    }

    @Override
    public void apply( final SVFeature featureArg, final Object header ) {
        final SVFeature feature;
        if ( noSampleFiltering ) {
            feature = featureArg;
        } else {
            feature = featureArg.extractSamples(sampleNames, header);
            if ( feature == null ) {
                return;
            }
        }
        if ( currentLocus != null &&
                IntervalUtils.compareLocatables(currentLocus, feature, getDictionary()) == 0 ) {
            sameLocusQueue.add(feature);
        } else {
            outputCodec.resolveSameLocusFeatures(sameLocusQueue, outputSink);
            currentLocus = feature;
            sameLocusQueue.add(feature);
        }
    }

    @Override
    public Object onTraversalSuccess() {
        super.onTraversalSuccess();
        outputCodec.resolveSameLocusFeatures(sameLocusQueue, outputSink);
        outputSink.close();
        return null;
    }
}
