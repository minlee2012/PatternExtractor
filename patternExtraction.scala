/**
* Min Lee
* Professor Grace Yang
* Big Data Analytics - COSC 282
*
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex

object patternExtraction {
	def main(args: Array[String]){
		val conf = new SparkConf().setMaster("local").setAppName("patternExtraction")
		val sc = new SparkContext(conf)//instantiates spark context

		//QUOTE REGEX
		//The quote regex takes in a quote and then anything that's not a quote and then the end quote. 
		val quotePattern = "\"[^\"]+\"".r

		//DATE REGEXES

		//The longform date was easy to code as a regex because if you have a number followed by a comma, space, and then a four-
		//digit number, chances are it's a date. Therefore, you can take whatever word precedes the first number, and there is a 
		//good chance it will be the month
		val dateLong = "\\S+\\s\\d+,\\s(\\d\\d\\d\\d)".r
		//The midform date was a bit tricker, as we had to differentiate this form from the shortform date, which is just the year.
		//If we followed the model of the longform date regex, we'd be picking up any random word that appeared before a year.
		//Therefore, although this is a bit messy, I just included all of the months as the only parameters that would make this
		//regex work.
		//****************************************************************************************************************
		//*Can we include a regex within a regex??* I tried this and it didn't work, but maybe my syntax was just off.
		//WHAT I TRIED:
		//val months = "\\b(January|February|March|April|May|June|July|August|September|October|November|December)\\b".r
		//val dateMid = ""months.r\\s(\\d\\d\\d\\d)"".r
		//****************************************************************************************************************
		val dateMid = "\\b(January|February|March|April|May|June|July|August|September|October|November|December)\\b\\s(\\d\\d\\d\\d)".r
		//The shortform date was easy: if you have four consequetive digits, it's most likely a year. Random numbers or monetary
		//values would have commas.
		val year = "(\\d\\d\\d\\d)".r

		//loading in files
		val file = sc.textFile("one.txt")
		val file2 = sc.textFile("two.txt")

		//combining the two files into one RDD.
		val text = file.union(file2)

		//Finding all of the quotes using the regex
		val quotesData = text.flatMap(a => quotePattern findAllIn a)
		val quotesFound = quotesData.flatMap(a => quotePattern findAllIn a)

		//Finding all of the date types using the regexes
		val datesLongFound = text.flatMap(a => dateLong findAllIn a)
		val datesMidFound = text.flatMap(a => dateMid findAllIn a)
		val yearsFound = text.flatMap(a => year findAllIn a)

		//Combining all of the dates into one RDD to make it easier to output.
		val datesFound = datesLongFound.union(datesMidFound)

		//println("Quotes Found: ")
		//quotesFound.foreach(println)
		print("Total Number of Quotes Found: ")
		println(quotesFound.count)

		//println("Years Found: ")
		//yearsFound.foreach(println)
		print("Total Number of Years Found: ")
		println(yearsFound.count)

		//println("Dates Found: ")
		//datesFound.foreach(println)
		print("Total Number of Dates Found: ")
		println(datesFound.count)

		sc.stop()//stops spark context

	}//main

}//patternExtraction