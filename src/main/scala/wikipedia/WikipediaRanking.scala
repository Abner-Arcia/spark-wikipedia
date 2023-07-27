package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Logger, Level}

import org.apache.spark.rdd.RDD
import scala.util.Properties.isWin

case class WikipediaArticle(title: String, text: String):
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)

object WikipediaRanking extends WikipediaRankingInterface:
  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf()
    .setMaster("local[2]") // local with two threads
    .setAppName("WikipediaRanking")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  // Map the lines, which are strings, to articles, and then create an rdd from the list of articles
  val wikipediaArticles: List[WikipediaArticle] = WikipediaData.lines.map(WikipediaData.parse)
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(wikipediaArticles)

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  /**
    * We give the zero value as 0 since we are dealing with numeric data
    * For the seqOp we pass a function that returns a updated acc if the article mentions the language or the same acc
    * if not. This gives the occurrences in a single partition
    * For the combOp we pass a function that adds the occurrences in each partition. This gives the total
    * occurrences
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.aggregate(0)(
    (acc, article) => { if article.mentionsLanguage(lang) then acc + 1 else acc },
    _+_
  )

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  /**
    * We map each language to a pair (language, occurrences of said language). Then we sort the list in descending order
    * comparing the values in each pair
    */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = langs.map(lang => {
    (lang, occurrencesOfLang(lang, rdd))
  }).sortWith(_._2 > _._2)

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  /**
    * We map each WikipediaArticle in the rdd to a list of tuples (language, article that mentions it) by first
    * filtering out the languages not mentioned in the article and then mapping each remaining language to a tuple. So,
    * for each article we get a list representing the languages mentioned in it
    * The previous operation would grant us an rdd of type RDD[List[(String, WikipediaArticle)]] but since we are using
    * flatMap we get an rdd of type RDD[(String, WikipediaArticle)] instead. This means that we have several tuples that
    * have the same key (language) but different values (articles)
    * And by using groupByKey we merge the tuples with the same language into a single tuple where the value becomes an
    * iterable that holds each article that each individual tuple had. We get an rdd of
    * (language, articles where that language is mentioned)
    */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    rdd.flatMap(article =>
      langs.filter(article.mentionsLanguage).map((_, article))
    ).groupByKey()

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  /**
    * With mapValues we give a function to transform each value in each tuple. That means mapping each iterable to its
    * size, thus getting an rdd of (language, occurrences). Then we collect it, convert it to a list and sort it
    */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index.mapValues(_.size).collect().toList.sortWith(_._2 > _._2)

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  /**
    * We do something similar as in makeIndex, but instead of creating tuples of (language, article) we create tuples of
    * (language, 1), expressing an occurrence of that language
    * Then, when we reduceByKey, the tuples with the same language are reduced to a single one, and the value for the
    * tuple is calculated by applying the function that we pass, which is adding the values of the tuples
    * After that we do the usual collecting, converting and sorting
    */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = rdd.flatMap(article =>
    langs.filter(article.mentionsLanguage).map((_, 1))
  ).reduceByKey(_+_).collect().toList.sortWith(_._2 > _._2)

  def main(args: Array[String]): Unit =

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
