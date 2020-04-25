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

public class CommonUtils {


    public static int millisToDay(long millisInUtc) {
        // You do NOT have to use this method, but it's provided as reference/example.
        // This "rounds millis down" to the calendar day, and you should use that to determine whether a person made
        // purchases for x consecutive days or not. You can find examples in unit tests.
        return (int) (millisInUtc / 1000L / 3600L / 24L);
    }


    public static class StrPrinter1 extends PTransform<PCollection<PurchaserProfile>, PDone> {
        final String prefix;

        public StrPrinter1(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<PurchaserProfile> input) {
            input.apply(ParDo.of(new DoFn<PurchaserProfile, Void>() {
                @ProcessElement public void process(ProcessContext c) {
                    PurchaserProfile pp = c.element();

//                    try {
//                        System.out.format("[%s] %s\n", prefix, ProtoUtils.getJsonFromMessage(c.element()));
//                    } catch (InvalidProtocolBufferException e) {
//                        e.printStackTrace();
//                    }



//                    if (pp.getId().getUuid().startsWith("13B53667") &&
//                        pp.getBundles(0).equals("id686486")) {
//
//                        try {
//                            System.out.format("[%s] %s\n", prefix, ProtoUtils.getJsonFromMessage(c.element()));
//                        } catch (InvalidProtocolBufferException e) {
//                            e.printStackTrace();
//                        }
//
//                    }



//                    if (c.element().getId().getUuid().startsWith("CBA25CC2")) {
//                        try {
//                            System.out.format("[%s] %s\n", prefix, ProtoUtils.getJsonFromMessage(c.element()));
//                        } catch (InvalidProtocolBufferException e) {
//                            e.printStackTrace();
//                        }
//                    }
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

//    public static class StrPrinter1New extends PTransform<PCollection<Profile.PurchaserProfileNew>, PDone> {
//        final String prefix;
//
//        public StrPrinter1New(String prefix) {
//            this.prefix = prefix;
//        }
//
//        @Override public PDone expand(PCollection<Profile.PurchaserProfileNew> input) {
//            input.apply(ParDo.of(new DoFn<Profile.PurchaserProfileNew, Void>() {
//                @DoFn.ProcessElement public void process(ProcessContext c) {
//                    try {
//                        System.out.format("[%s] %s\n", prefix, ProtoUtils.getJsonFromMessage(c.element()));
//                    } catch (InvalidProtocolBufferException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }));
//            return PDone.in(input.getPipeline());
//        }
//    }

    public static class StrPrinter2 extends PTransform<PCollection<DeviceId>, PDone> {
        final String prefix;

        public StrPrinter2(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<DeviceId> input) {
            input.apply(ParDo.of(new DoFn<DeviceId, Void>() {
                @ProcessElement public void process(ProcessContext c) {

                    System.out.format("[%s] %s, %s\n", prefix, c.element().getOs(), c.element().getUuid());


//                    try {
//                        System.out.format("[%s] %s\n", prefix, ProtoUtils.getJsonFromMessage(c.element()));
//                    } catch (InvalidProtocolBufferException e) {
//                        e.printStackTrace();
//                    }
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class StrPrinter3 extends PTransform<PCollection<KV<DeviceId, PurchaserProfile>>, PCollection<KV<DeviceId, PurchaserProfile>>> {
        final String prefix;

        public StrPrinter3(String prefix) {
            this.prefix = prefix;
        }

        @Override public PCollection<KV<DeviceId, PurchaserProfile>> expand(PCollection<KV<DeviceId, PurchaserProfile>> input) {
            input.apply(ParDo.of(new DoFn<KV<DeviceId, PurchaserProfile>, Void>() {
                @ProcessElement public void process(ProcessContext c) {
                    try {
                        System.out.format("[%s] %s\n", prefix + " v", ProtoUtils.getJsonFromMessage(c.element().getValue()));
                        System.out.format("[%s] %s\n", prefix + " k", ProtoUtils.getJsonFromMessage(c.element().getKey()));
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
            }));
            return input;
        }
    }


    public static class StrPrinter4 extends PTransform<PCollection<InAppPurchaseProfile>, PDone> {
        final String prefix;

        public StrPrinter4(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<InAppPurchaseProfile> input) {
            input.apply(ParDo.of(new DoFn<InAppPurchaseProfile, Void>() {
                @ProcessElement public void process(ProcessContext c) {
                try {
                    System.out.format("[%s] %s\n", prefix, ProtoUtils.getJsonFromMessage(c.element()));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class StrPrinterBase64 extends PTransform<PCollection<PurchaserProfile>, PDone> {
        final String prefix;

        public StrPrinterBase64(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<PurchaserProfile> input) {
            input.apply(ParDo.of(new DoFn<PurchaserProfile, Void>() {
                @ProcessElement public void process(ProcessContext c) {

                    try {
                        System.out.format("[%s] %s\n", prefix, ProtoUtils.encodeMessageBase64(c.element()));
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class StrPrinter5 extends PTransform<PCollection<DeviceProfile>, PDone> {
        final String prefix;

        public StrPrinter5(String prefix) {
            this.prefix = prefix;
        }

        @Override public PDone expand(PCollection<DeviceProfile> input) {
            input.apply(ParDo.of(new DoFn<DeviceProfile, Void>() {
                @ProcessElement public void process(ProcessContext c) {
                    System.out.format("[%s] %s\n", prefix, c.element().getDeviceId().getUuid());
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class StrPrinter6 extends PTransform<PCollection<KV<DeviceId, float[]>>, PDone> {
        final String prefix;

        public StrPrinter6(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public PDone expand(PCollection<KV<DeviceId, float[]>> input) {
            input.apply(ParDo.of(new DoFn<KV<DeviceId, float[]>, Void>() {
                @ProcessElement public void process(ProcessContext c) {
                    try {
                        System.out.format("[%s] %s, float: %s\n",
                            prefix,
                            ProtoUtils.getJsonFromMessage(c.element().getKey(), true),
                            c.element().getValue().toString());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }



}














