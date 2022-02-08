package org.broadinstitute.hellbender.engine;

import htsjdk.samtools.util.Log;
import htsjdk.tribble.Feature;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class FeatureMergingWalkerUnitTest extends CommandLineProgramTest {
    private static final class DummyFeatureWalker extends FeatureMergingWalker<Feature> {
        @Argument( fullName = StandardArgumentDefinitions.FEATURE_LONG_NAME,
                    shortName = StandardArgumentDefinitions.FEATURE_SHORT_NAME )
        private List<FeatureInput<Feature>> featureInputs;

        final List<Feature> features = new ArrayList<>();
        @Override public void apply( Feature feature, Object header ) {
            features.add(feature);
        }
    }

    @Test
    public void testDictionarySubsetIsOK() {
        final DummyFeatureWalker dummy = new DummyFeatureWalker();
        final String[] args = {
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, Log.LogLevel.ERROR.name(),
                // full HG38 dictionary with 3366 entries
                "--" + StandardArgumentDefinitions.SEQUENCE_DICTIONARY_NAME, FULL_HG38_DICT,
                // subset of HG38 dictionary with 13 entries
                "--" + StandardArgumentDefinitions.REFERENCE_LONG_NAME, b38_reference_20_21,
                // bam has full HG38 dictionary
                "-" + StandardArgumentDefinitions.INPUT_SHORT_NAME, largeFileTestDir + "NA12878.alignedHg38.duplicateMarked.baseRealigned.bam",
                // no dictionary, no sample names, with a single feature
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_hg38.baf.txt"
        };
        dummy.instanceMain(args);

        // full HG38 dictionary has 3366 entries
        Assert.assertEquals(dummy.getDictionary().size(), 3366);
        // feature file has 1 feature, no samples, no dictionary
        Assert.assertEquals(dummy.features.size(), 1);
        Assert.assertEquals(dummy.getSampleNames().size(), 0);
    }

    @Test(expectedExceptions = { UserException.class })
    public void testMixedDictionariesAreNotOK() {
        final DummyFeatureWalker dummy = new DummyFeatureWalker();
        final String[] args = {
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, Log.LogLevel.ERROR.name(),
                "--" + StandardArgumentDefinitions.DISABLE_SEQUENCE_DICT_VALIDATION_NAME, "true",
                // full HG38 dictionary with 3366 entries
                "--" + StandardArgumentDefinitions.SEQUENCE_DICTIONARY_NAME, FULL_HG38_DICT,
                // subset of HG37 dictionary has conflicting names
                "--" + StandardArgumentDefinitions.REFERENCE_LONG_NAME, b37_reference_20_21,
                // no dictionary, no sample names, with a single feature
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_hg38.baf.txt"
        };
        dummy.instanceMain(args);
    }

    @Test(expectedExceptions = { UserException.class })
    public void testMisorderedDictionariesAreNotOK() {
        final DummyFeatureWalker dummy = new DummyFeatureWalker();
        final String[] args = {
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, Log.LogLevel.ERROR.name(),
                // subset of HG38 dictionary with misordered contigs
                "--" + StandardArgumentDefinitions.SEQUENCE_DICTIONARY_NAME, packageRootTestDir + "engine/wrongOrder.dict",
                // subset of HG38 dictionary with 13 entries
                "--" + StandardArgumentDefinitions.REFERENCE_LONG_NAME, b38_reference_20_21,
                // no dictionary, no sample names, with a single feature
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_hg38.baf.txt"
        };
        dummy.instanceMain(args);
    }

    @Test
    public void testGetDictionaryAndSamplesFromBCIFile() {
        final DummyFeatureWalker dummy = new DummyFeatureWalker();
        final String[] args = {
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, Log.LogLevel.ERROR.name(),
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_hg38.baf.bci"
        };
        dummy.instanceMain(args);
        // bci file has chr20, chr21, and alts for those contigs: 13 entries
        Assert.assertEquals(dummy.getDictionary().size(), 13);
        // feature file has 1 feature
        Assert.assertEquals(dummy.features.size(), 1);
        // feature file has 1 sample name
        Assert.assertEquals(dummy.getSampleNames().size(), 1);
    }

    @Test
    public void testContigsOrder() {
        final DummyFeatureWalker dummy = new DummyFeatureWalker();
        final String[] args = {
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, Log.LogLevel.ERROR.name(),
                // full HG38 dictionary with 3366 entries
                "--" + StandardArgumentDefinitions.SEQUENCE_DICTIONARY_NAME, FULL_HG38_DICT,
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_hg38.baf.txt",
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_chr9.baf.txt",
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_chr10.baf.txt"
        };
        dummy.instanceMain(args);
        Assert.assertEquals(dummy.features.size(), 5);
        Assert.assertEquals(dummy.features.get(0).getContig(), "chr9");
        Assert.assertEquals(dummy.features.get(1).getContig(), "chr10");
        Assert.assertTrue(dummy.features.get(1).getStart() <= dummy.features.get(2).getStart());
        Assert.assertEquals(dummy.features.get(2).getContig(), "chr10");
        Assert.assertTrue(dummy.features.get(2).getStart() <= dummy.features.get(3).getStart());
        Assert.assertEquals(dummy.features.get(3).getContig(), "chr10");
        Assert.assertEquals(dummy.features.get(4).getContig(), "chr21");
    }

    @Test
    public void testCoordinateOrder() {
        final DummyFeatureWalker dummy = new DummyFeatureWalker();
        final String[] args = {
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, Log.LogLevel.ERROR.name(),
                // full HG38 dictionary with 3366 entries
                "--" + StandardArgumentDefinitions.SEQUENCE_DICTIONARY_NAME, FULL_HG38_DICT,
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_chr10.baf.txt",
                "-" + StandardArgumentDefinitions.FEATURE_SHORT_NAME, packageRootTestDir + "engine/tiny_chr10_2.baf.txt"
        };
        dummy.instanceMain(args);
        Assert.assertEquals(dummy.features.size(), 5);
        int lastStart = -1;
        for ( final Feature feature : dummy.features ) {
            Assert.assertTrue(feature.getStart() >= lastStart );
            lastStart = feature.getStart();
        }
    }
}
