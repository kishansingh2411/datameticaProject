package com.od.data.service.rest.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author sandeep
 * 
 */
public enum AggregatorLevel {
  DAY("date", "yyyy-MM-dd", "yyyyMMdd", "yyyy-MM-dd"), WEEK("week", "YYYY'W'ww", "YYYYww", "YYYY'W'ww"), MONTH("month", "yyyy-MM",
      "yyyyMM", "yyyy-MM"), QUARTER("quarter", "yyyy'Q'qq", "yyyyqq", "yyyy'Q'qq"), YEAR("year", "yyyy", "yyyy", "yyyy");

  private String displayName;
  private String responseDateFormat;
  private String queryColumnFormat;
  private String[] validRequestParams;

  private AggregatorLevel(String displayName, String responseDateFormat, String queryColumnFormat, String... validRequestParam) {
    this.displayName = displayName;
    this.responseDateFormat = responseDateFormat;
    this.queryColumnFormat = queryColumnFormat;
    this.validRequestParams = validRequestParam;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getQueryColumnFormat() {
    return queryColumnFormat;
  }

  public String[] getValidRequestParams() {
    return validRequestParams;
  }

  public String getResponseDateFormat() {
    return responseDateFormat;
  }

  public String getPrimaryDatePattern() {
    return validRequestParams[0];
  }

  public static AggregatorLevel getAggregateLevel(String name) {
    AggregatorLevel aggregatorLevel = null;
    for (AggregatorLevel level : AggregatorLevel.values()) {
      if (level.getDisplayName().equalsIgnoreCase(name)) {
        aggregatorLevel = level;
        break;
      }
    }

    return aggregatorLevel;
  }

  public static AggregatorLevel getDefault() {
    return DAY;
  }

  public static Set<String> getAllSupportedDatePatterns() {
    Set<String> validPatterns = new LinkedHashSet<String>();
    AggregatorLevel[] levels = AggregatorLevel.values();
    for (AggregatorLevel level : levels) {
      for (String pattern : level.validRequestParams) {
        validPatterns.add(pattern);
      }
    }
    return validPatterns;
  }

  public static Map<String, List<AggregatorLevel>> getPatternToAggregatorLevelsMap(boolean considerOnlyPrimaryPattern) {
    Map<String, List<AggregatorLevel>> map = new HashMap<String, List<AggregatorLevel>>();
    AggregatorLevel[] levels = AggregatorLevel.values();
    for (AggregatorLevel level : levels) {
      for (String pattern : level.validRequestParams) {
        List<AggregatorLevel> list = map.get(pattern);
        if (null == list) {
          list = new ArrayList<AggregatorLevel>(3);
          map.put(pattern, list);
        }
        list.add(level);
        if (considerOnlyPrimaryPattern) {
          break;
        }
      }
    }
    return map;
  }
}
