/* NewEggLCAttributionModel.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
//import java.io.File
import scala.tools._
//import java.nio.file._


//------------------------------------
//
//  Last Click Aquisition Model Scala/Spark
//  General by Month
//  7.26.14
//  update: 8.29.14
//  by: j.mcerlain
//
//------------------------------------


object lastClickAttModel {
  def main(args: Array[String]){ 
    val partnerIDs        = args(0)
    val timeOffSetMinutes = args(1).toInt
    val filePathToWrite   = args(2)
    val nbDaysToProcess   = args(3).toInt
    val nowParam          = args(4)

    val conf = new SparkConf().setAppName("Last Attribution Model")
      val sc = new SparkContext(conf)
      
  // Extract Dates and Format Strings for drill down into the Hive MetaStore

      object DateExtract extends java.io.Serializable {
      // change depending on which segment you with wish to catupure, ie weekly, monthly etc.
      def datesBetween(startDate: DateTime, endDate: DateTime): Seq[DateTime] = {
        var daysBetween = Days.daysBetween(startDate.toDateMidnight(), endDate.toDateMidnight()).getDays()
        1 to daysBetween map { startDate.withFieldAdded(DurationFieldType.days(), _ )} 
        }
      def hoursBetween(startDate: DateTime, endDate: DateTime): Seq[DateTime] = {
        var hourBetween = Hours.hoursBetween(startDate, endDate).getHours()
        1 to hourBetween map { startDate.withFieldAdded(DurationFieldType.hours(), _ )} 
        }
      def parseDate(line: String): DateTime = {
        var fmt = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        fmt.parseDateTime(line)
        }
      }
      var now = DateTime.now().minus(Period.hours(1))

      if (nowParam != "now") now = new DateTime(nowParam).plus(Period.minutes(timeOffSetMinutes))
      
      val then_hours = now.minus(Period.days(nbDaysToProcess)).minus(Period.minutes(timeOffSetMinutes)).toDateMidnight.toDateTime.plusMinutes(timeOffSetMinutes)
      val hours_bw = DateExtract.hoursBetween(then_hours,now)
      

      def getDatesFormatted(line: Int): (String) = {
          def concat(ss: String*) = ss filter (_.nonEmpty) mkString ""
          if(line <= 9) concat("0",line.toString)
          else line.toString
      }
      val h = partnerIDs.toString.split(',')
      //val h = List(args(0)) // Should be Passed as Parameters

  // Generates strings of file structure within time frame from above, 
  // then checks that file paths exist in HDFS and reads data into RDDs
  // then generates one larger Unioned RDD from the smaller parts

      val s = "hdfs:/hive/tracking.db/fingerprint_h/partnerid=WW/partyear=QQ/partmonth=XX/partday=YY/parthour=ZZ/"
      def format(p: String, m: String, n: String, o: String) = s.replace("QQ",p).replace("XX",m).replace("YY",n).replace("ZZ",o)
      def form_hours(list: Seq[DateTime]): Seq[String] ={
        list.map(l => (format(getDatesFormatted(l.getYear),getDatesFormatted(l.getMonthOfYear),getDatesFormatted(l.getDayOfMonth),getDatesFormatted(l.getHourOfDay))))
      }
  // Check files structure of HDFS:
      val confH = new Configuration()
      val fileSystem = FileSystem.get(confH)
      def checkFile(filename: String) = {
          val path = new Path(filename)
          if ((fileSystem.exists(path)) == true) filename
          else null
      }

      val toHours = form_hours(hours_bw)  
      val final_hr = for (l <- h; m <- toHours) yield m.replace("WW",l) //Drill into partnerID folders
      // def checkFiles(line: String)= if (Files.exists(Paths.get(line)) == true) line else null
      val check = final_hr.map(l => checkFile(l))
      val checkf = check.filter(l => l != null)
      val checkedFiles = checkf.map(l => l.toString.replace("hdfs:/","hdfs:///"))
      val RDDs = checkedFiles.map(l => sc.textFile(l))
      val data = sc.union(RDDs)

  // EXRACT FINGERPRINTS   

      var array1 = data.map(line => line.split('\t')) 
      def extractKey(line: String): (String) = {
            val reg_code = """^.*ref=https?%3A%2F%2F([w]{3}\.)?(.*?)%2F.*?$""".r
            reg_code.findFirstIn(line) match {
              case Some(reg_code(www,ref)) =>
                if (ref != "\"-\"") (ref)
                else ("unknown")
              case _ => ("unknown")
            }
          }      
    
      def extractPartnerID(line: String): (String) = {
           var reg_code = """rtlr=(\d{3,7})""".r
           reg_code.findFirstIn(line) match {
              case Some(reg_code(oID)) =>
                if (oID != "\"-\"") (oID)
                else ("unknown")
              case _ => ("unknown")
            }
          }      
       
 // Final Ref Source    
      var x1 = array1.map(l => (l(13),l(27),extractPartnerID(l(9)),extractKey(l(9)))).cache()  //SPUID1st, Sessionid, PID, Refereral Source      
 
 // EXTRACT TRANSACTIONS
      print("Extracting Transactions from Trans_Item Table   ")
      // val now = DateTime.now().minus(Period.hours(2))
      // val then_hours = now.minus(Period.days(60))
      // val hours_bw = DateExtract.hoursBetween(then_hours,now)
      // val t = "hdfs:///hive/tracking.db/transaction_item_h/partnerid=112/partyear=2014/partmonth=XX/partday=YY/*"
      // def formatt(m: String, n: String) = t.replace("XX",m).replace("YY",n)
      // val toDatest = days_bw_week.map(l => (formatt(getDatesFormatted(l.getMonthOfYear),getDatesFormatted(l.getDayOfMonth))))

      val s1 = "hdfs:/hive/tracking.db/transaction_h/partnerid=WW/partyear=QQ/partmonth=XX/partday=YY/parthour=ZZ/"
      def format1(p: String, m: String, n: String, o: String) = s1.replace("QQ",p).replace("XX",m).replace("YY",n).replace("ZZ",o)
      def form_hours1(list: Seq[DateTime]): Seq[String] ={
        list.map(l => (format1(getDatesFormatted(l.getYear),getDatesFormatted(l.getMonthOfYear),getDatesFormatted(l.getDayOfMonth),getDatesFormatted(l.getHourOfDay))))
      }
      val toHourst = form_hours1(hours_bw)  
      val final_hrt = for (l <- h; m <- toHourst) yield m.replace("WW",l) //Drill into partnerID folders
      val checkt = final_hrt.map(l => checkFile(l))
      val checkt2 = checkt.filter(l => l != null)
      val checkedFilest = checkt2.map(l => l.toString.replace("hdfs:/","hdfs:///"))
      val RDDst = checkt2.map(l => sc.textFile(l))
      //val RDDst2 = RDDst.collect().map()
      val datat = sc.union(RDDst)
      val datat2 = datat.collect()


 // Test Lines of data to make sure they are the correct length and therefore have all the needed data
      var array2 = datat.map(line => (line.split('\t'), line.split('\t').length))
      var array_1 = array2.filter(l => l._2 == 30)
      var array_2 = array_1.map(l => (l._1))      


 // Extract Data from RDD and format date to DateTime Object and subtracts the args(1) argument ie the offset from GMT
      print("Extract Data from RDD and Format to DateTime Objects    ")
      def extractOrderID(line: String): (String) = {
           var reg_code = """transid=(\d{3,7})""".r
           reg_code.findFirstIn(line) match {
              case Some(reg_code(oID)) =>
                if (oID != "\"-\"") (oID)
                else ("unknown")
              case _ => ("unknown")
            }
          }      
              
      var z = array_2.map(l => (l(13),l(29),DateExtract.parseDate(l(6)).minusMinutes(timeOffSetMinutes),extractOrderID(l(9)))) //Spuid1st, Sessionid, db_friendly_date, orderid, //element, Count, Price      
 
 //CLEANING FOR JOIN BY KEY  
      print("Joining Data Sets from Fingerprints and Transactions as Hash Key Values   ")
      var byKeyX = x1.map({case (id, sess, pid, ref) => (id, sess)->(ref, pid)}).cache()
      var byKeyZ = z.map({case (id, sess, dt, oID) => (id, sess)->(dt, oID)}).cache()      
 
 //JOINING
      var joined = byKeyX.join(byKeyZ).cache()
      var x = joined.map({case ((id, sess),((ref,pid),(dt, oID))) => (ref) -> (pid, sess, id, dt, oID)}).cache() 

 //Reduce by Referer      
      //var raw_att = x.reduceByKey(_ + _)      
 
 // Clean Source  
      print("Getting CleanSource Data from hive/jimmy.db/clean_source3/*    ")    
      var file_source = sc.textFile("hdfs:///hive/jimmy.db/clean_source3/*")
      var file_source_sp = file_source.map(l => l.split(','))
      var file_source_spa = file_source_sp.map(l => (l(1),l(2)))
      var clean_source = file_source_spa.map({case (t1, t2) => (t1) -> (t2)})
      var cleaned_att = x.join(clean_source).cache()

 // Drop Session ID and SPUID1st here:     
      val data1 = cleaned_att.map({case (ref,((pid, sess, id, dt, oID), ref_clean)) => (ref, pid, sess, id, dt, oID, ref_clean)})
 
 // Marketing Source, Referral URL, PartnerID, Date timestamp, Year, Month, Day, Hour, OrderID
      print("Cleaning data and exporting to hdfs:///Data/tmp/01    ")
      val dataFinal = data1.map(l => (l._7 + "," + l._1 + "," + l._2 + "," + l._5 + "," + l._5.getYear + "," + l._5.getMonthOfYear + "," + l._5.getDayOfMonth + "," + l._5.getHourOfDay + "," + l._6))
      dataFinal.coalesce(1).saveAsTextFile(filePathToWrite)

  }
}

