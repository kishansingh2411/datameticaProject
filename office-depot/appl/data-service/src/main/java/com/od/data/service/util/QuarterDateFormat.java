/**
 * 
 */
package com.od.data.service.util;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author sandeep
 * 
 */
public class QuarterDateFormat extends DateFormat {

  private static final long serialVersionUID = 1L;

  private class CharacterOccurrenceInfo {

    private char characer;
    private int startIndex;
    private int endIndex;
    private int count;

    private CharacterOccurrenceInfo(char characer, int startIndex, int endIndex) {
      this.characer = characer;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.count = this.endIndex - startIndex + 1;
    }

    @SuppressWarnings("unused")
    public char getCharacer() {
      return characer;
    }

    public int getStartIndex() {
      return startIndex;
    }

    public int getEndIndex() {
      return endIndex;
    }

    public int getCount() {
      return count;
    }

    @SuppressWarnings("unused")
    public String getPattern() {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < count; i++) {
        sb.append(characer);
      }
      return sb.toString();
    }
  }

  private class RegExRule {

    private String regEx;
    private Integer matchCount;

    public RegExRule(String regEx) {
      this(regEx, null);
    }

    public RegExRule(String regEx, Integer matchCount) {
      this.regEx = regEx;
      this.matchCount = matchCount;
    }

    public String getRegEx() {
      return regEx;
    }

    public Integer getMatchCount() {
      return matchCount;
    }

    @Override
    public String toString() {
      return "RegExRule [regEx=" + regEx + ", matchCount=" + matchCount + "]";
    }
  }

  private static final Character QUOTED_CHARACTER = 'Q';

  private static final Set<Character> SUPPORTED_CHAR_SET = new HashSet<Character>(2);
  static {
    SUPPORTED_CHAR_SET.add('y');
    SUPPORTED_CHAR_SET.add('q');
  }

  private String pattern;

  private CharacterOccurrenceInfo yearCharInfo;

  private CharacterOccurrenceInfo quarterCharInfo;

  RegExRule[] regExRules = {new RegExRule("y*[^A-Za-z0-9]*Q??q{1,2}+[^A-Za-z0-9]*y*"), new RegExRule("yyyy", 1), new RegExRule("y", 4)};

  public QuarterDateFormat(String pattern) {
    this(pattern, TimeZone.getDefault());
  }

  public QuarterDateFormat(String pattern, TimeZone timeZone) {
    this.calendar = new GregorianCalendar(timeZone);
    pattern = pattern.replaceAll("'" + QUOTED_CHARACTER + "'", String.valueOf(QUOTED_CHARACTER));
    if (isValidPattern(pattern)) {
      this.pattern = pattern;
    } else {
      throw new IllegalArgumentException("Invalid pattern: " + pattern);
    }
    compilePattern();
  }

  private void compilePattern() {
    this.yearCharInfo = new CharacterOccurrenceInfo('y', pattern.indexOf('y'), pattern.lastIndexOf('y'));
    this.quarterCharInfo = new CharacterOccurrenceInfo('q', pattern.indexOf('q'), pattern.lastIndexOf('q'));
  }

  @Override
  public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
    this.calendar.setTime(date);
    int year = this.calendar.get(Calendar.YEAR);
    int month = this.calendar.get(Calendar.MONTH);
    int quarter = month / 3;

    StringBuffer sb = new StringBuffer();
    sb.append(pattern);
    substituteValue(sb, yearCharInfo, year);
    substituteValue(sb, quarterCharInfo, quarter + 1);

    return toAppendTo.append(sb);
  }

  @Override
  public Date parse(String source, ParsePosition pos) {

    int sIndex = pos.getIndex();
    int eIndex = sIndex + pattern.length() - 1;
    if (eIndex > source.length()) {
      pos.setErrorIndex(sIndex);
      return null;
    }

    String strToParse = source.substring(sIndex, eIndex + 1);
    for (int i = 0; i < strToParse.length(); i++) {
      char ch = strToParse.charAt(i);
      if (pattern.charAt(i) != ch) {
        if (!SUPPORTED_CHAR_SET.contains(pattern.charAt(i)) || !Character.isDigit(ch)) {
          pos.setErrorIndex(i);
          return null;
        }
      }
    }

    Date date = null;

    try {
      int year = Integer
          .parseInt(strToParse.substring(yearCharInfo.getStartIndex(), yearCharInfo.getStartIndex() + yearCharInfo.getCount()));
      int quarter = Integer.parseInt(strToParse.substring(quarterCharInfo.getStartIndex(), quarterCharInfo.getStartIndex()
          + quarterCharInfo.getCount()));
      if (year < 0 || quarter < 1 || quarter > 4) {
        pos.setErrorIndex(0);
        return null;
      }
      calendar.set(Calendar.YEAR, year);
      calendar.set(Calendar.MONTH, (quarter - 1) * 3);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      date = calendar.getTime();
    } catch (Exception e) {
      pos.setErrorIndex(0);
      return null;
    }
    pos.setIndex(eIndex + 1);
    return date;
  }

  private boolean isValidPattern(String inputPattern) {
    boolean isValid = true;
    int passCount = 0;

    for (RegExRule rule : regExRules) {
      Pattern pattern = Pattern.compile(rule.getRegEx());
      Matcher matcher = pattern.matcher(inputPattern);
      if (null != rule.getMatchCount()) {
        int matchCounter = 0;
        for (; matcher.find(); matchCounter++);
        isValid = matchCounter == rule.getMatchCount().intValue();
      } else {
        isValid = matcher.matches();
      }
      if (isValid) {
        passCount++;
      }
    }

    return passCount == regExRules.length;
  }

  private void substituteValue(StringBuffer sb, CharacterOccurrenceInfo charInfo, int value) {
    int eIndex = charInfo.getEndIndex();
    char[] cArray = String.valueOf(value).toCharArray();
    int cEndIndex = cArray.length - 1;

    for (int i = 0; i < charInfo.getCount(); i++) {
      if (i <= cEndIndex) {
        sb.setCharAt(eIndex - i, cArray[cEndIndex - i]);
      } else {
        sb.setCharAt(eIndex - i, '0');
      }
    }
  }

}
