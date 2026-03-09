/* NewEggLCAttributionModel.scala */
//
//  Last Click Acquisition Model - Scala/Spark
//  General by Month
//  7.26.14  update: 8.29.14
//  by: j.mcerlain
//

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object DateUtils extends java.io.Serializable {
  private val dateFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def hoursBetween(start: DateTime, end: DateTime): Seq[DateTime] = {
    val count = Hours.hoursBetween(start, end).getHours()
    1 to count map { start.withFieldAdded(DurationFieldType.hours(), _) }
  }

  def parseDate(s: String): DateTime = dateFmt.parseDateTime(s)

  def pad2(n: Int): String = f"$n%02d"
}

object RegexExtract {
  private val refPattern    = """^.*ref=https?%3A%2F%2F([w]{3}\.)?(.*?)%2F.*?$""".r
  private val partnerPattern = """rtlr=(\d{3,7})""".r
  private val orderPattern   = """transid=(\d{3,7})""".r

  private def extractFirst(pattern: scala.util.matching.Regex, line: String): String =
    pattern.findFirstMatchIn(line).map(_.group(pattern.groupCount)).filter(_ != "\"-\"").getOrElse("unknown")

  def referralKey(line: String): String = refPattern.findFirstIn(line) match {
    case Some(refPattern(_, ref)) if ref != "\"-\"" => ref
    case _ => "unknown"
  }

  def partnerID(line: String): String  = extractFirst(partnerPattern, line)
  def orderID(line: String): String    = extractFirst(orderPattern, line)
}

object lastClickAttModel {

  val EXPECTED_COLS = 30

  def hdfsPathsForHours(baseTemplate: String, partnerIds: Array[String], hours: Seq[DateTime]): Seq[String] = {
    val formatted = hours.map { h =>
      import DateUtils.pad2
      baseTemplate
        .replace("QQ", pad2(h.getYear))
        .replace("XX", pad2(h.getMonthOfYear))
        .replace("YY", pad2(h.getDayOfMonth))
        .replace("ZZ", pad2(h.getHourOfDay))
    }
    for (pid <- partnerIds; path <- formatted) yield path.replace("WW", pid)
  }

  def existingHdfsPaths(paths: Seq[String], fs: FileSystem): Seq[String] =
    paths
      .filter(p => fs.exists(new Path(p)))
      .map(_.replace("hdfs:/", "hdfs:///"))

  def main(args: Array[String]) {
    val partnerIds      = args(0).split(',')
    val offsetMinutes   = args(1).toInt
    val outputPath      = args(2)
    val daysToProcess   = args(3).toInt
    val nowParam        = args(4)

    val sc = new SparkContext(new SparkConf().setAppName("Last Attribution Model"))
    val fs = FileSystem.get(new Configuration())

    // --- Time window ---
    val now = if (nowParam == "now")
      DateTime.now().minus(Period.hours(1))
    else
      new DateTime(nowParam).plus(Period.minutes(offsetMinutes))

    val windowStart = now
      .minus(Period.days(daysToProcess))
      .minus(Period.minutes(offsetMinutes))
      .toDateMidnight.toDateTime
      .plusMinutes(offsetMinutes)

    val hours = DateUtils.hoursBetween(windowStart, now)

    // --- Load fingerprint data ---
    val fingerprintTemplate = "hdfs:/hive/tracking.db/fingerprint_h/partnerid=WW/partyear=QQ/partmonth=XX/partday=YY/parthour=ZZ/"
    val fingerprintPaths = existingHdfsPaths(hdfsPathsForHours(fingerprintTemplate, partnerIds, hours), fs)
    val fingerprintData = sc.union(fingerprintPaths.map(sc.textFile(_)))

    // (spuid1st, sessionId, partnerID, referralSource)
    val fingerprints = fingerprintData
      .map(_.split('\t'))
      .map(cols => (cols(13), cols(27), RegexExtract.partnerID(cols(9)), RegexExtract.referralKey(cols(9))))
      .cache()

    // --- Load transaction data ---
    print("Extracting Transactions from Trans_Item Table   ")
    val transactionTemplate = "hdfs:/hive/tracking.db/transaction_h/partnerid=WW/partyear=QQ/partmonth=XX/partday=YY/parthour=ZZ/"
    val transactionPaths = existingHdfsPaths(hdfsPathsForHours(transactionTemplate, partnerIds, hours), fs)
    val transactionData = sc.union(transactionPaths.map(sc.textFile(_)))

    // Filter to rows with full column count, then extract fields
    print("Extract Data from RDD and Format to DateTime Objects    ")
    val transactions = transactionData
      .map(_.split('\t'))
      .filter(_.length == EXPECTED_COLS)
      .map(cols => (cols(13), cols(29), DateUtils.parseDate(cols(6)).minusMinutes(offsetMinutes), RegexExtract.orderID(cols(9))))
      .cache()

    // --- Join fingerprints and transactions on (spuid1st, sessionId) ---
    print("Joining Data Sets from Fingerprints and Transactions as Hash Key Values   ")
    val byKeyFingerprints  = fingerprints.map   { case (id, sess, pid, ref) => (id, sess) -> (ref, pid) }.cache()
    val byKeyTransactions  = transactions.map   { case (id, sess, dt, oID)  => (id, sess) -> (dt, oID)  }.cache()

    // (referralSource -> (partnerID, sessionId, spuid1st, date, orderID))
    val joined = byKeyFingerprints.join(byKeyTransactions)
      .map { case ((id, sess), ((ref, pid), (dt, oID))) => ref -> (pid, sess, id, dt, oID) }
      .cache()

    // --- Join with clean source lookup ---
    print("Getting CleanSource Data from hive/jimmy.db/clean_source3/*    ")
    val cleanSource = sc.textFile("hdfs:///hive/jimmy.db/clean_source3/*")
      .map(_.split(','))
      .map(cols => cols(1) -> cols(2))

    // (ref, partnerID, sessionId, spuid1st, date, orderID, cleanRef)
    val attributed = joined.join(cleanSource)
      .map { case (ref, ((pid, sess, id, dt, oID), cleanRef)) => (ref, pid, sess, id, dt, oID, cleanRef) }

    // --- Output: Marketing Source, Referral URL, PartnerID, Date, Year, Month, Day, Hour, OrderID ---
    print(s"Cleaning data and exporting to $outputPath    ")
    attributed
      .map { case (ref, pid, _, _, dt, oID, cleanRef) =>
        s"$cleanRef,$ref,$pid,$dt,${dt.getYear},${dt.getMonthOfYear},${dt.getDayOfMonth},${dt.getHourOfDay},$oID"
      }
      .coalesce(1)
      .saveAsTextFile(outputPath)
  }
}
