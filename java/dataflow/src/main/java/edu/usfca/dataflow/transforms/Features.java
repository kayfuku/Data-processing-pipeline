package edu.usfca.dataflow.transforms;

import java.util.Map;

import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.CommonUtils;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.dataflow.utils.PredictionUtils;
import edu.usfca.protobuf.Profile;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.DeviceProfile;

public class Features {
  /**
   * This PTransform takes a PCollectionList that contains three PCollections of Strings.
   *
   * 1. DeviceProfile (output from the first pipeline) with unique DeviceIDs,
   *
   * 2. DeviceId (output from the first pipeline) that are "suspicious" (call this SuspiciousIDs), and
   *
   * 3. InAppPurchaseProfile (separately provided) with unique bundles.
   *
   * All of these proto messages are Base64-encoded (you can check ProtoUtils class for how to decode that, e.g.).
   *
   * [Step 1] First, in this PTransform, you must filter out (remove) DeviceProfiles whose DeviceIDs are found in the
   * SuspiciousIDs as we are not going to consider suspicious users.
   *
   * [Step 2] Next, you ALSO filter out (remove) DeviceProfiles whose DeviceID's UUIDs are NOT in the following form:
   *
   * ???????0-????-????-????-????????????
   *
   * Effectively, this would "sample" the data at rate (1/16). This sampling is mainly for efficiency reasons (later
   * when you run your pipeline on GCP, the input data is quite large as you will need to make "predictions" for
   * millions of DeviceIDs).
   *
   * To be clear, if " ...getUuid().charAt(7) == '0' " is true, then you process the DeviceProfile; otherwise, ignore
   * it.
   *
   * [Step 3] Then, for each user (DeviceProfile), use the method in
   * {@link edu.usfca.dataflow.utils.PredictionUtils#getInputFeatures(DeviceProfile, Map)} to obtain the user's
   * "Features" (to be used for TensorFlow model). See the comments for this method.
   *
   * Note that the said method takes in a Map (in addition to DeviceProfile) from bundles to IAPP, and thus you will
   * need to figure out how to turn PCollection into a Map. We have done this in the past (in labs & lectures).
   *
   */
  public static class GetInputToModel extends PTransform<PCollectionList<String>, PCollection<KV<DeviceId, float[]>>> {

    @Override
    public PCollection<KV<DeviceId, float[]>> expand(PCollectionList<String> pcList) {
      // TODO: If duplicate deviceIDs are found, throw CorruptedDataException.
      // Note that UUIDs are case-insensitive.

      // Decode input DeviceProfiles.
      PCollection<DeviceProfile> deviceProfiles = pcList.get(0) // PC<String>
        .apply(MapElements.into(TypeDescriptor.of(DeviceProfile.class)).via((String b64) -> {
          try {
            return ProtoUtils.decodeMessageBase64(DeviceProfile.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        })) // PC<DeviceProfile>
        .apply(Filter.by((ProcessFunction<DeviceProfile, Boolean>) dp ->
            !dp.equals(DeviceProfile.getDefaultInstance())));

      // Decode input SupiciousIDs and make it a map view.
      PCollectionView<Map<DeviceId, Integer>> suspiciousIdsView = pcList.get(1) // PC<String>
        .apply(MapElements.into(TypeDescriptor.of(DeviceId.class)).via((String b64) -> {
          try {
            return ProtoUtils.decodeMessageBase64(DeviceId.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        })) // PC<DeviceId>
        .apply(Filter.by((ProcessFunction<DeviceId, Boolean>) deviceId ->
            !deviceId.equals(DeviceId.getDefaultInstance()))) // PC<DeviceId>
        .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptor.of(DeviceId.class), TypeDescriptors.integers()))
            .via((DeviceId deviceId) -> KV.of(deviceId, 1))) // PC<KV<DeviceId, Integer (dummy)>>
        .apply(View.asMap());

      // Decode input InAppPurchaseProfile and make it a map view.
      PCollectionView<Map<String, InAppPurchaseProfile>> iappsView = pcList.get(2) // PC<String>
        .apply(MapElements.into(TypeDescriptor.of(InAppPurchaseProfile.class)).via((String b64) -> {
          try {
            return ProtoUtils.decodeMessageBase64(InAppPurchaseProfile.parser(), b64);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          return null;
        })) // PC<InAppPurchaseProfile>
        .apply(Filter.by((ProcessFunction<InAppPurchaseProfile, Boolean>) iapp ->
            !iapp.equals(InAppPurchaseProfile.getDefaultInstance()))) // PC<InAppPurchaseProfile>
        .apply(MapElements.into(TypeDescriptors.kvs(
          TypeDescriptors.strings(), TypeDescriptor.of(InAppPurchaseProfile.class)))
          .via((InAppPurchaseProfile iapp) -> KV.of(iapp.getBundle(), iapp))) // PC<KV<String (bundle), InAppPurchaseProfile>>
        .apply(View.asMap());

      // Remove duplicate deviceIDs.
      deviceProfiles.apply(ParDo.of(new DeviceProfileUtils.GetDeviceId())) // PC<KV<DeviceId, DeviceProfile>>
        .apply(Count.perKey()) // PC<KV<DeviceId, Long>>
        .apply(Filter.by((ProcessFunction<KV<DeviceId, Long>, Boolean>) kv -> {
          if (kv.getValue() > 1L) {
            throw new CorruptedDataException("Duplicate DeviceIDs found in dp PC.");
          }
          return kv.getValue() == 1L;
        }));

      PCollection<DeviceProfile> dpSampled = deviceProfiles // PC<DeviceProfile>
        // [Step 1] Remove suspicious IDs.
        .apply(ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {

          Map<DeviceId, Integer> suspiciousIdsMap;

          @ProcessElement
          public void filter(@Element DeviceProfile dp, OutputReceiver<DeviceProfile> out, ProcessContext c) {
            if (suspiciousIdsMap == null) {
              suspiciousIdsMap = c.sideInput(suspiciousIdsView);
            }
            if (!suspiciousIdsMap.containsKey(dp.getDeviceId())) {
              out.output(dp);
            }
          }

        }).withSideInputs(suspiciousIdsView)) // PC<DeviceProfile>
        // [Step 2] Sampling
        .apply(Filter.by((ProcessFunction<DeviceProfile, Boolean>) dp -> dp.getDeviceId().getUuid().charAt(7) == '0'));

      // [Step 3]
      PCollection<KV<DeviceId, float[]>> inputToModel = dpSampled // PC<DeviceProfile>
        .apply(ParDo.of(new DoFn<DeviceProfile, KV<DeviceId, float[]>>() {

          Map<String, InAppPurchaseProfile> iappMap;

          @ProcessElement
          public void anyName(@Element DeviceProfile dp, OutputReceiver<KV<DeviceId, float[]>> out, ProcessContext c) {
            if (iappMap == null) {
              iappMap = c.sideInput(iappsView);
            }
            out.output(KV.of(dp.getDeviceId(), PredictionUtils.getInputFeatures(dp, iappMap)));
          }
        }).withSideInputs(iappsView));

      return inputToModel;
    }
  }
}
