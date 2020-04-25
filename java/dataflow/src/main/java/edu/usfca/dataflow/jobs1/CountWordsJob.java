package edu.usfca.dataflow.jobs1;

import edu.usfca.dataflow.utils.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.usfca.dataflow.MyOptions;

public class CountWordsJob {
  private static final Logger LOG = LoggerFactory.getLogger(CountWordsJob.class);

  /**
   * ---------------------------------------------------------------
   *
   * Instructions (Task A)
   *
   * ---------------------------------------------------------------
   *
   * 1. Before you begin writing any code, run unit tests found in java/judge.
   *
   * Specifically, pay attention to all unit tests in "__Test01*"-"__Test09*" classes (for Task A).
   *
   * Read the comments found in each of those files, as they give you an idea of what you should work on to pass them.
   *
   * 1-1. "__Test01BidLogUtils" Fix silly bugs in BidLogUtils class.
   *
   * 1-2. "__Test02DeviceProfileUtils" Fix silly bugs in DeviceProfileUtils class.
   *
   * 2. As you work on PTransforms, you will repeat what you did in Project 2. Note that the initial input (BidLogs) is
   * given in TFRecord format, and you should review L31 (sample code) for using TFRecordIO. Given the starter code, you
   * really do not need to write a lot of code (unless you want to write everything from scratch). Instead of writing
   * your own, I strongly suggest you use the provided starter code as a starting point (correctness first, optimization
   * later).
   *
   * 2-1. "__Test03BidLogJob" This tests BidLog to DeviceProfiles.
   *
   * 2-2. "__Test04BidLogJob" This tests AppProfiles.
   *
   * 2-3. "__Test05BidLogJob" This tests Suspicious Users.
   *
   * 2-4. "__Test06BidLogJob" This tests Suspicious Users.
   *
   * 2-5. "__Test07" through "__Test09" use many BidLogs to test the entire pipeline (all outputs). If your pipeline
   * passes all unit tests up to __Test06 but fails on __Test07-09, perhaps there's a subtle bug in your pipeline (which
   * may lead to non-deterministic behaviors). Make sure that you debug your pipeline using IDE's debugger & by printing
   * messages/data to the console.
   *
   * 2-6. If all unit tests pass, run your job locally (using DirectRunner) by using the following command:
   *
   * Under "java/dataflow" directory (of your local repo):
   *
   * gradle run -Pargs="--job=bidLogJob --bidLogCountThreshold=115 --geoCountThreshold=1 --userCountThreshold=2
   * --appCountThreshold=18 --pathToResourceRoot=/Users/haden/usf/resources/project5-actual"
   *
   * (Note that the last flag can be omitted if that's the path you already set in Main method.)
   *
   * 
   * 3. When this job runs successfully (either on your machine or on GCP), it should output three files, one file under
   * each subdirectory of the "output" directory of your LOCAL_PATH_TO_RESOURCE_DIR (if you ran it locally).
   *
   * 3-1. DeviceProfile: Merged DeviceProfile data (per user=DeviceId). We'll treat this dataset as "lifetime"
   * DeviceProfile dataset. Revisit Project 2 for the details (or see DeviceProfileUtils class). You should see 707
   * lines in the file.
   *
   * 3-2. AppProfile: AppProfile data (generated using DeviceProfile data from 3-1). To simplify things, we are only
   * counting lifetime user count and also count per exchange. You should see 510 lines in the file.
   *
   * 3-3. Suspicious (Device): If a certain device has *a lot of apps* that few people use, the said device may be a bot
   * (not a normal human user). In this project, a DeviceId is considered "suspicious" if it has enough unpopular apps
   * (where an app is unpopular if the unique number of users is small). In addition, if a device "appeared" in too many
   * geological locations, that's also considered suspicious. You should see 6 lines in the file.
   *
   * 3-4. For all three datasets mentioned above, you'll write text files in Base64 encoding. Some of these will be used
   * in the second job (pipeline), called PredictionJob.
   *
   * Note that "sample output" files are provided (in the "output-reference" directory), as they should be used as input
   * to the second pipeline, but the contents of the sample output could be different from the contents of your output
   * (e.g., the order of the lines can be different as PCollections do not preserve the order of elements).
   *
   * 4. Before you begin to optimize your "BidLog2DeviceProfile" PTransform, you must ensure that all unit tests pass.
   *
   * In Task C, you will run your job on GCP, and your score for Task C will be based on the efficiency of your
   * pipeline. Further instructions will be provided after you complete Task A.
   */

  public static class Log2CommentText extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {

      PCollection<String> commentTexts = input // PC<String>
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void parse(@Element String jsonLogAsLine, OutputReceiver<String> out) {
            String commentText = LogParser.getComment(jsonLogAsLine);
            out.output(commentText);
          }
        })); // PC<String>

      return commentTexts;
    }
  }

  public static void execute(MyOptions options) {
    LOG.info("Options: {}", options.toString());
    final PathConfigs config = PathConfigs.of(options);
    Pipeline p = Pipeline.create(options);

    // 1. Read Reddit comment data.
    PCollection<String> rawData = p.apply("Read", TextIO.read().from(config.getReadPathToRedditComment()));
    PCollection<String> commentTexts = rawData.apply("Parse", new Log2CommentText());

    commentTexts.apply(new CommonUtils.StrPrinter1("comment text"));




//    // 2. Obtain AppProfiles.
//    PCollection<AppProfile> appProfiles = deviceProfiles.apply("ComputeAppProfiles", new ComputeAppProfiles());
//
//    // 3. Suspicious users (IDs).
//    PCollection<DeviceId> suspiciousUsers = getSuspiciousIDs(deviceProfiles, appProfiles, //
//        options.getUserCountThreshold(), options.getAppCountThreshold(), options.getGeoCountThreshold(),
//        options.getBidLogCountThreshold());
//
//    // 4. Ouput.
//    IOUtils.encodeB64AndWrite(deviceProfiles, config.getWritePathToDeviceProfile());
//    IOUtils.encodeB64AndWrite(appProfiles, config.getWritePathToAppProfile());
//    IOUtils.encodeB64AndWrite(suspiciousUsers, config.getWritePathToSuspiciousUser());

    // 4. Output (write to GCS).
    // For convenience, we'll use Base64 encoding.
    // TODO: Uncomment the following, and use the right parameters (this will be necessary for Task C, in particular).
    // IOUtils.encodeB64AndWrite(deviceProfiles, config.getWritePathToDeviceProfile());
    // IOUtils.encodeB64AndWrite(appProfiles, config.get...());
    // IOUtils.encodeB64AndWrite(suspiciousUsers, config.get...());

    p.run().waitUntilFinish();
  }
}
