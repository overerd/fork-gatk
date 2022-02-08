package org.broadinstitute.hellbender.tools.sv;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.testutils.IntegrationTestSpec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;

public class MergeSVEvidenceIntegrationTest extends CommandLineProgramTest {
    @Test
    public void testCorrectFeatureTypes() throws IOException {
        final IntegrationTestSpec testSpec = new IntegrationTestSpec(
                " -" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME + " %s" +
                " -" + StandardArgumentDefinitions.FEATURE_SHORT_NAME + " " + packageRootTestDir + "engine/tiny_hg38.baf.bci",
                Collections.singletonList(packageRootTestDir + "engine/tiny_hg38.baf.txt"));
        testSpec.setOutputFileExtension("baf.txt");
        testSpec.executeTest("matching input and output types", this);
    }

    @Test
    public void testIncorrectFeatureTypes() throws IOException {
        final IntegrationTestSpec testSpec = new IntegrationTestSpec(
                " -" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME + " %s" +
                        " -" + StandardArgumentDefinitions.FEATURE_SHORT_NAME + " " + packageRootTestDir + "engine/tiny_hg38.baf.bci",
                1, UserException.class);
        testSpec.setOutputFileExtension("pe.txt");
        testSpec.executeTest("mismatched input and output types", this);
    }
}
