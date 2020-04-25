package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.dataflow.utils.DeviceProfileUtils.GetDeviceId;
import edu.usfca.protobuf.Bid;
import edu.usfca.protobuf.Bid.Exchange;
import edu.usfca.protobuf.Common;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Iterator;

public class AppProfiles {
  /**
   * "ComputeAppProfiles" takes in one PCollection of DeviceProfiles.
   *
   * If the input PCollection contains any duplicate Device IDs (recall that uuid is case-insensitive),
   *
   * then it must throw "CorruptedDataException".
   *
   * Otherwise, proceed to produce one AppProfile proto per "bundle" (String, case-sensitive).
   *
   * For each bundle (app), you should aggregate:
   *
   * (1) "bundle": This is unique key (String) for each AppProfile, and is case-sensitive.
   *
   * (2) "user_count": This is the unique number of users (Device IDs) who have this app in their DeviceProfile's
   * AppActivity.
   *
   * (3) "user_count_per_exchange": Same as (2), but it's a map from "Exchange" enum (its integer value) to the number
   * of unique DeviceIDs.
   *
   * (Note that this is simplified when compared to Project 2.)
   *
   * TODO: You can use instructor's reference code from project 2 and modify it (you'll need to fix a couple of things),
   * or reuse yours. Note that either way you'll have to make changes because the requirements / proto definitions have
   * changed slightly (things are simplified).
   */
  public static class ComputeAppProfiles extends PTransform<PCollection<DeviceProfile>, PCollection<AppProfile>> {

    @Override
    public PCollection<AppProfile> expand(PCollection<DeviceProfile> input) {

      // Remove duplicate device ids.
      input.apply(ParDo.of(new GetDeviceId()))
        .apply(Count.perKey())
        .apply(Filter.by((ProcessFunction<KV<Common.DeviceId, Long>, Boolean>) kv -> {
          if (kv.getValue() > 1L) {
            throw new CorruptedDataException("Duplicate DeviceIDs found in input PC.");
          }
          return true; // <= ok?
        }));

      // To count the num of device profiles per bundle and num of device profiles per exchange for each bundle
      final int offset = 2;
      final int arrSize = 22 + 1 + offset;

      return input
        .apply(ParDo.of(new EmitData())) // PC<KV<String (bundle), Integer (exchange/TO_COUNT)>>
        .apply(Count.perElement()) // PC<KV<KV<String (bundle), Integer (exchange/TO_COUNT)>, Long (num of the keys)>>
        .apply(ParDo.of(new DoFn<KV<KV<String, Integer>, Long>, KV<String, KV<Integer, Long>>>() {
          @ProcessElement
          public void process(ProcessContext c) {
            c.output(
                KV.of(c.element().getKey().getKey(),
                    KV.of(c.element().getKey().getValue(), c.element().getValue())));
          }
        })) // PC<KV<String (bundle), KV<Integer (exchange/TO_COUNT), Long (num of the keys)>>>
        .apply(Combine.perKey(new CombineFn<KV<Integer, Long>, int[], AppProfile>() {
          @Override
          public int[] createAccumulator() {
            return new int[arrSize];
          }

          @Override
          public int[] addInput(int[] mutableAccumulator, KV<Integer, Long> input) {
            mutableAccumulator[input.getKey() + offset] += input.getValue().intValue();
            return mutableAccumulator;
          }

          @Override
          public int[] mergeAccumulators(Iterable<int[]> accumulators) {
            Iterator<int[]> it = accumulators.iterator();
            int[] first = null;
            while (it.hasNext()) {
              int[] next = it.next();
              if (first == null) {
                first = next;
              } else {
                for (int i = 0; i < arrSize; i++) {
                  first[i] += next[i];
                }
              }
            }

            return first;
          }

          @Override
          public AppProfile extractOutput(int[] accumulator) {
            AppProfile.Builder ap = AppProfile.newBuilder();
            ap.setUserCount(accumulator[TO_COUNT + offset]);
            for (Exchange exchange : Exchange.values()) {
              if (exchange == Exchange.UNRECOGNIZED || accumulator[exchange.getNumber() + offset] == 0) {
                continue;
              }
              ap.putUserCountPerExchange(exchange.getNumber(), accumulator[exchange.getNumber() + offset]);
            }
            return ap.build();
          }
        })) // PC<KV<String (bundle), AppProfile>>
        .apply(MapElements.into(TypeDescriptor.of(AppProfile.class))
            .via((KV<String, AppProfile> x) -> x.getValue().toBuilder().setBundle(x.getKey()).build()));
            // PC<AppProfile>
    }

    // To count num of device profiles per bundle
    static final int TO_COUNT = -1;

    // PC<DeviceProfile> => PC<KV<String (bundle), Integer (exchange/LIFE_COUNT?)>>
    static class EmitData extends DoFn<DeviceProfile, KV<String, Integer>> {
      @ProcessElement
      public void process(ProcessContext c) {
        DeviceProfile dp = c.element();
        for (DeviceProfile.AppActivity app : dp.getAppList()) {
          c.output(KV.of(app.getBundle(), TO_COUNT)); // To count num of device profiles per bundle
          for (int exchange : app.getCountPerExchangeMap().keySet()) {
            if (exchange < 0 || app.getCountPerExchangeMap().get(exchange) == 0) {
              continue;
            }
            // To count the num of device profiles per exchange for each bundle
            c.output(KV.of(app.getBundle(), exchange));
          }
        }
      }

    }

  }

}
