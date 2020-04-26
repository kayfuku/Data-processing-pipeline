package edu.usfca.dataflow.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.Arrays;

public class CommonUtils {

    public static class StrPrinter1 extends PTransform<PCollection<String>, PDone> {
        final String prefix;

        public StrPrinter1(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<String> input) {
            input.apply(ParDo.of(new DoFn<String, Void>() {
                @ProcessElement public void process(ProcessContext c) {

                    System.out.format("[%s] %s\n", prefix, c.element());

                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class StrPrinter2 extends PTransform<PCollection<String[]>, PDone> {
        final String prefix;

        public StrPrinter2(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<String[]> input) {
            input.apply(ParDo.of(new DoFn<String[], Void>() {
                @ProcessElement public void process(ProcessContext c) {

                    System.out.format("[%s] %s\n", prefix, Arrays.toString(c.element()));

                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

}














