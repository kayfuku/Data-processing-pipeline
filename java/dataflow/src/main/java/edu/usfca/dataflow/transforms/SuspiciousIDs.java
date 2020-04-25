package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.utils.CommonUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.Profile.DeviceProfile.AppActivity;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SuspiciousIDs {
  private static final Logger LOG = LoggerFactory.getLogger(SuspiciousIDs.class);

  /**
   * This method serves to flag certain users as suspicious.
   *
   * (1) USER_COUNT_THRESHOLD: This determines whether an app is popular or not.
   *
   * Any app that has more than "USER_COUNT_THRESHOLD" unique users is considered popular.
   *
   * Default value is 4 (so, 5 or more users = popular).
   *
   *
   * (2) APP_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "APP_COUNT_THRESHOLD" unpopular apps,
   *
   * then the user is considered suspicious.
   *
   * Default value is 3 (so, 4 or more unpopular apps = suspicious).
   *
   *
   * (3) GEO_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "GEO_COUNT_THRESHOLD" unique Geo's,
   *
   * then the user is considered suspicious.
   *
   * Default value is 8 (so, 9 or more distinct Geo's = suspicious).
   *
   * 
   * (4) BID_LOG_COUNT_THRESHOLD: If a user (DeviceProfile) appeared in more than "BID_LOG_COUNT_THRESHOLD" Bid Logs,
   *
   * then the user is considered suspicious (we're not counting invalid BidLogs for this part as it should have been
   * ignored from the beginning).
   *
   * Default value is 10 (so, 11 or more valid BidLogs from the same user = suspicious).
   *
   * 
   * NOTE: When you run your pipelines on GCP, we'll not use the default values for these thresholds (see the document).
   *
   * The default values are mainly for unit tests (so you can easily check correctness with rather small threshold
   * values).
   */
  public static PCollection<DeviceId> getSuspiciousIDs(//
      PCollection<DeviceProfile> dps, //
      PCollection<AppProfile> aps, //
      int USER_COUNT_THRESHOLD, // Default is 4 for unit tests.
      int APP_COUNT_THRESHOLD, // Default is 3 for unit tests.
      int GEO_COUNT_THRESHOLD, // Default is 8 for unit tests.
      int BID_LOG_COUNT_THRESHOLD // Default is 10 for unit tests.
  ) {
    LOG.info("[Thresholds] user count {} app count {} geo count {} bid log count {}", USER_COUNT_THRESHOLD,
        APP_COUNT_THRESHOLD, GEO_COUNT_THRESHOLD, BID_LOG_COUNT_THRESHOLD);

    // Get unpopular apps using PCollection view (map).
    final PCollectionView<Map<String, Integer>> unpopularBundlesView = getUnpopularApps(aps, USER_COUNT_THRESHOLD) // PC<String>
      .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
          .via((String bundle) -> KV.of(bundle, 0))) // PC<KV<String (bundle), Integer (dummy)>>
      .apply(View.asMap());

    TupleTag<DeviceProfile> deviceProfilesTag = new TupleTag<DeviceProfile>() {};
    TupleTag<DeviceId> tooManyUnpopularAppsDevIdsTag = new TupleTag<DeviceId>() {};
    TupleTag<DeviceId> tooManyGeosDevIdsTag = new TupleTag<DeviceId>() {};
    TupleTag<DeviceId> tooManyBidLogsDevIdsTag = new TupleTag<DeviceId>() {};

    // Filter for unpopular apps
    PCollectionTuple tuple1 = dps // PC<DeviceProfile>
      .apply(ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {

      Map<String, Integer> unpopMap;

      @ProcessElement
      public void filter(@Element DeviceProfile dp, MultiOutputReceiver out, ProcessContext c) {
        if (unpopMap == null) {
          unpopMap = c.sideInput(unpopularBundlesView);
        }
        int numUnpopularApps = 0;
        for (AppActivity app : dp.getAppList()) {
          if (unpopMap.containsKey(app.getBundle())) {
            numUnpopularApps++;
          }
        }
        if (numUnpopularApps > APP_COUNT_THRESHOLD) {
          out.get(tooManyUnpopularAppsDevIdsTag).output(dp.getDeviceId());
          return;
        }
        out.get(deviceProfilesTag).output(dp);
      }
      }).withOutputTags(deviceProfilesTag, TupleTagList.of(tooManyUnpopularAppsDevIdsTag))
        .withSideInputs(unpopularBundlesView));

    // Filter for geo
    PCollectionTuple tuple2 = tuple1.get(deviceProfilesTag) // PC<DeviceProfile>
      .apply(ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {
        @ProcessElement
        public void filter(@Element DeviceProfile dp, MultiOutputReceiver out, ProcessContext c) {
          if (dp.getGeoList().size() > GEO_COUNT_THRESHOLD) {
            out.get(tooManyGeosDevIdsTag).output(dp.getDeviceId());
            return;
          }
          out.get(deviceProfilesTag).output(dp);
        }
      }).withOutputTags(deviceProfilesTag, TupleTagList.of(tooManyGeosDevIdsTag)));

    // Filter for bid logs
    PCollectionTuple tuple3 = tuple2.get(deviceProfilesTag) // PC<DeviceProfile>
      .apply(ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {
        @ProcessElement
        public void filter(@Element DeviceProfile dp, MultiOutputReceiver out, ProcessContext c) {
          int numBidLogs = 0;
          for (AppActivity app : c.element().getAppList()) {
            for (Integer count : app.getCountPerExchangeMap().values()) {
              numBidLogs += count;
            }
          }
          if (numBidLogs > BID_LOG_COUNT_THRESHOLD) {
            out.get(tooManyBidLogsDevIdsTag).output(dp.getDeviceId());
            return;
          }
          out.get(deviceProfilesTag).output(dp);
        }
      }).withOutputTags(deviceProfilesTag, TupleTagList.of(tooManyBidLogsDevIdsTag))); // PCTuple

    // Get the suspicious device ids.
    PCollection<DeviceId> tooManyUnpopularAppsDevIds = tuple1.get(tooManyUnpopularAppsDevIdsTag);
    PCollection<DeviceId> tooManyGeosDevIds = tuple2.get(tooManyGeosDevIdsTag);
    PCollection<DeviceId> tooManyBidLogsDevIds = tuple3.get(tooManyBidLogsDevIdsTag);

    // Flatten.
    PCollection<DeviceId> suspiciousDevIds = PCollectionList.of(tooManyUnpopularAppsDevIds)
        .and(tooManyGeosDevIds).and(tooManyBidLogsDevIds) // PCList<DeviceId>
      .apply(Flatten.pCollections()); // PC<DeviceId>


    return suspiciousDevIds;
  }

  public static PCollection<String> getUnpopularApps(PCollection<AppProfile> aps, int userCount) {
    return aps
      .apply(ParDo.of(new DoFn<AppProfile, String>() {
        @ProcessElement
        public void anyName(@Element AppProfile ap, OutputReceiver<String> out) {
          if (ap.getUserCount() <= userCount) {
            out.output(ap.getBundle());
          }
        }
      }));
  }

}
