package org.apache.spark.sql.catalyst.json
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.charset.{Charset, StandardCharsets}
import java.time.ZoneId
import java.util.Locale
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.catalyst.util.PermissiveMode

import scala.collection.JavaConverters._

/**
 * __KUP__
 * 
 * [JsonOptions] represented the downgraded [JSONOptions]
 * class from v2.4.x available in v.2.1.3 and higher
 * 
 * Options for parsing JSON data into Spark SQL rows.
 *
 * Most of these map directly to Jackson's internal options, 
 * specified in [[JsonParser.Feature]].
 */
class JsonOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends Serializable  {

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String) = {
      this(
        CaseInsensitiveMap(parameters),
        defaultTimeZoneId,
        defaultColumnNameOfCorruptRecord)
  }

  def this(
      parameters: java.util.Map[String,String],
      defaultTimeZoneId: String = "UTC",
      defaultColumnNameOfCorruptRecord: String = "") = {
      this(parameters.asScala.toMap, defaultTimeZoneId, defaultColumnNameOfCorruptRecord)
      
  }

  /*********************************
   * 
   * DateTimeUtils.scala
   * 
   * The subsequent parameters  & method
   * are part of DateTimeUtils of v2.4.4
   * 
   */
  val TIMEZONE_OPTION = "timeZone"
  
  def getZoneId(timeZoneId: String): ZoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)

  /********************************/
  
  /*
   * __KUP__
   * 
   * caseSensitive has been added to support resolvers
   * in JsonInferSchema class
   */
  val caseSensitive: Boolean =
    parameters.get("caseSensitive").exists(_.toBoolean)

  val samplingRatio: Double =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  val primitivesAsString: Boolean =
    parameters.get("primitivesAsString").exists(_.toBoolean)
  val prefersDecimal: Boolean =
    parameters.get("prefersDecimal").exists(_.toBoolean)
  val allowComments: Boolean =
    parameters.get("allowComments").exists(_.toBoolean)
  val allowUnquotedFieldNames: Boolean =
    parameters.get("allowUnquotedFieldNames").exists(_.toBoolean)
  val allowSingleQuotes: Boolean =
    parameters.get("allowSingleQuotes").forall(_.toBoolean)
  val allowNumericLeadingZeros: Boolean =
    parameters.get("allowNumericLeadingZeros").exists(_.toBoolean)
  val allowNonNumericNumbers: Boolean =
    parameters.get("allowNonNumericNumbers").forall(_.toBoolean)
  val allowBackslashEscapingAnyCharacter: Boolean =
    parameters.get("allowBackslashEscapingAnyCharacter").exists(_.toBoolean)
  private val allowUnquotedControlChars =
    parameters.get("allowUnquotedControlChars").exists(_.toBoolean)
  val compressionCodec: Option[String] = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val columnNameOfCorruptRecord: String =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)

  // Whether to ignore column of all null values or empty array/struct during schema inference
  val dropFieldIfAllNull: Boolean = parameters.get("dropFieldIfAllNull").exists(_.toBoolean)

  // Whether to ignore null fields during json generating
  val ignoreNullFields: Boolean = parameters.get("ignoreNullFields").exists(_.toBoolean)

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get("locale").map(Locale.forLanguageTag).getOrElse(Locale.US)

  val zoneId: ZoneId = getZoneId(
    parameters.getOrElse(TIMEZONE_OPTION, defaultTimeZoneId))

  val dateFormat: String = parameters.getOrElse("dateFormat", "uuuu-MM-dd")

  val timestampFormat: String =
    parameters.getOrElse("timestampFormat", "uuuu-MM-dd'T'HH:mm:ss.SSSXXX")

  val multiLine: Boolean = parameters.get("multiLine").exists(_.toBoolean)
  println(multiLine)

  /**
   * A string between two consecutive JSON records.
   */
  val lineSeparator: Option[String] = parameters.get("lineSep").map { sep =>
    require(sep.nonEmpty, "'lineSep' cannot be an empty string.")
    sep
  }

  protected def checkedEncoding(enc: String): String = enc

  /**
   * Standard encoding (charset) name. For example UTF-8, UTF-16LE and UTF-32BE.
   * If the encoding is not specified (None) in read, it will be detected automatically
   * when the multiLine option is set to `true`. If encoding is not specified in write,
   * UTF-8 is used by default.
   */
  val encoding: Option[String] = parameters.get("encoding")
    .orElse(parameters.get("charset")).map(checkedEncoding)

  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.map { lineSep =>
    lineSep.getBytes(encoding.getOrElse(StandardCharsets.UTF_8.name()))
  }
  val lineSeparatorInWrite: String = lineSeparator.getOrElse("\n")

  /**
   * Generating JSON strings in pretty representation if the parameter is enabled.
   */
  val pretty: Boolean = parameters.get("pretty").exists(_.toBoolean)

  /**
   * Enables inferring of TimestampType from strings matched to the timestamp pattern
   * defined by the timestampFormat option.
   */
  val inferTimestamp: Boolean = parameters.get("inferTimestamp").forall(_.toBoolean)

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, allowUnquotedControlChars)
  }
}

class JsonOptionsInRead(
    @transient override val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends JsonOptions(parameters, defaultTimeZoneId, defaultColumnNameOfCorruptRecord) {

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String = "") = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      defaultColumnNameOfCorruptRecord)
  }

  protected override def checkedEncoding(enc: String): String = {
    val isBlacklisted = JsonOptionsInRead.blacklist.contains(Charset.forName(enc))
    require(multiLine || !isBlacklisted,
      s"""The ${enc} encoding must not be included in the blacklist when multiLine is disabled:
         |Blacklist: ${JsonOptionsInRead.blacklist.mkString(", ")}""".stripMargin)

    val isLineSepRequired =
        multiLine || Charset.forName(enc) == StandardCharsets.UTF_8 || lineSeparator.nonEmpty
    require(isLineSepRequired, s"The lineSep option must be specified for the $enc encoding")

    enc
  }
}

object JsonOptionsInRead {
  // The following encodings are not supported in per-line mode (multiline is false)
  // because they cause some problems in reading files with BOM which is supposed to
  // present in the files with such encodings. After splitting input files by lines,
  // only the first lines will have the BOM which leads to impossibility for reading
  // the rest lines. Besides of that, the lineSep option must have the BOM in such
  // encodings which can never present between lines.
  val blacklist = Seq(
    Charset.forName("UTF-16"),
    Charset.forName("UTF-32")
  )
}