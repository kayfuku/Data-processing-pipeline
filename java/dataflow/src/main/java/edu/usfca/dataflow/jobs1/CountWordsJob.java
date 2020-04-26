package edu.usfca.dataflow.jobs1;

import edu.usfca.dataflow.utils.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.usfca.dataflow.MyOptions;

public class CountWordsJob {
  private static final Logger LOG = LoggerFactory.getLogger(CountWordsJob.class);

  /**
   * Parse.
   */
  public static class Log2CommentText extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {

      PCollection<String> commentTexts = input // PC<String>
        // Get "body" field value in the JSON format.
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void parse(@Element String jsonLogAsLine, OutputReceiver<String> out) {
            String commentText = LogParser.getComment(jsonLogAsLine);
            out.output(commentText);
          }
        })) // PC<String>
        // Filter out [deleted] or [removed].
        .apply(Filter.by((ProcessFunction<String, Boolean>) ct -> {
          if (StringUtils.isBlank(ct)) {
            return false;
          }
          if (ct.equals("[deleted]") || ct.equals("[removed]")) {
            return false;
          }
          return true;
        })); // PC<String>

      return commentTexts;
    }
  }

  /**
   * Generate N grams.
   */
  public static class GetNGram extends PTransform<PCollection<String>, PCollection<String[]>> {

    private static int N;

    GetNGram(int n) {
      this.N = n;
    }

    private static String[] generateNgram(int N, String texts) {
      String[] tokens = texts.split("\\s+");
      String[] ngram = new String[N];

      // Generate N grams.
      for (int k = 0; k < (tokens.length - N + 1); k++) {
        int start = k;
        int end = k + N;
        int i = 0;
        for (int j = start; j < end; j++) {
          ngram[i] = tokens[j];
          i++;
        }
      }

      return ngram;
    }

    @Override
    public PCollection<String[]> expand(PCollection<String> commentTexts) {

      PCollection<String[]> ngrams = commentTexts // PC<String>
        .apply(ParDo.of(new DoFn<String, String[]>() {
          @ProcessElement
          public void parse(@Element String texts, OutputReceiver<String[]> out) {
            String[] ngram = generateNgram(N, texts);
            out.output(ngram);
          }
        })); // PC<String[]>

      return ngrams;
    }
  }


  public static void execute(MyOptions options) {
    LOG.info("Options: {}", options.toString());
    final PathConfigs config = PathConfigs.of(options);
    Pipeline p = Pipeline.create(options);

    // 1. Read Reddit comment data and filter out some types of texts.
    PCollection<String> rawData = p.apply("Read", TextIO.read().from(config.getReadPathToRedditComment()));
    PCollection<String> commentTexts = rawData.apply("Parse", new Log2CommentText());

//    commentTexts.apply(new CommonUtils.StrPrinter1("comment text"));

    // 2. Generate N grams.
    PCollection<String[]> ngrams = commentTexts.apply("GetNGram", new GetNGram(2));

    // Warning about coders!
//    ngrams.apply(new CommonUtils.StrPrinter2("ngram"));

    // 3. Count words.
    








    p.run().waitUntilFinish();
  }
}
