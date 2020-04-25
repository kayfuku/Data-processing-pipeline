package edu.usfca.dataflow.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Event.PurchaseEvent;
import edu.usfca.protobuf.Event.PurchaseEvent.Store;

/**
 * Parse JSON to get "body" field (comment text).
 */
public class LogParser {

  final static JsonParser parser = new JsonParser();

  public static String getComment(String jsonLogAsLine) {
    try {
      JsonObject jsonLog = parser.parse(jsonLogAsLine).getAsJsonObject();
      final String commentText = jsonLog.get("body").getAsString();
      return commentText;

    } catch (Exception eee) {
      return null;
    }
  }

}
