// Databricks notebook source
import scala.collection.mutable.ListBuffer

// COMMAND ----------

//argument 1 : Which will give the folder that consist of all the training files
val training="/FileStore/tables/training"
//argument 2: Which will give the testfile which consist of the words.Any one particular file from the testing data set
val testingfile="/FileStore/tables/53068"
//argument 3: Since I am taking the filename instead of the path, this gives which length to access the document 4 in this case because /FileStore/tables/test/ (3+1(can access the file)=4)
val n=4
//argumnet 4:Count of the training files in the directory.Here I am testing only with 2 files
val count=2

// COMMAND ----------

//For temporary usage
val temp=sc.wholeTextFiles(training).flatMap {
      case (path, text) =>
        text.split("""\W+""")
          .map {
            word => (word, path)
          }
    }.map {
  //Note this parameter changes on the size of the number of items in the list
      case (w, p) => ((w, p.split("/")(n)), 1)
    }.reduceByKey {
      case (n1, n2) => n1 + n2
    }

// COMMAND ----------

//term frequency
val tf=temp.map {
      case ((w, p), n) => (w, (p, n))
    }.groupBy {
      case (w, (p, n)) => w
    }.map {
      case (w, seq) =>
        val seq2 = seq map {
          case (_, (p, n)) => (p, n)
        }
        (w, seq2.mkString(", "))
    }
tf.take(10)

// COMMAND ----------

//document frequency
val df=temp.map{
  x=>(x._1)
}.map
{
  x=>(x._1,1)
}
.reduceByKey(_+_)
df.take(10)

// COMMAND ----------

//Joining for the sake of obtaining tf-idf
val tfidf = tf.join(df)
tfidf.take(10)

// COMMAND ----------

//Calculating the Weights
val final_weight = for(a <- tfidf)
yield {
  val word=a._1
  val docf=a._2._2
  val values = a._2._1
  val rddvalues = values.split(" ")
  val s = for(b <- rddvalues)
  yield {
    val w = b.split(",")(1).replace("(","").replace(")","")
    val docpath = b.split(",")(0).replace("(","").replace(")","")
    var weight=w.toInt*scala.math.log(count/docf.toInt)
    var listb1 = new ListBuffer[String]()
    listb1 += word
    listb1 += docpath
    listb1 += weight.toString
    listb1
  }
  s
}
final_weight.take(10)

// COMMAND ----------

//The Final List which consists of the (word,document_path,weights)
val tfidf_final= final_weight.flatMap(x=>x).collect()

// COMMAND ----------

//Printing all the tf-idf values for the words in the textfile
val testing = sc.textFile(testingfile)
val testwords=testing.flatMap(x=>x.split("\\s+"))
var testSet = testwords.collect.toSet
val weightsList = for(i<- 0 to tfidf_final.length-1)
yield {
  val a = tfidf_final(i)(0)
  if(testSet.contains(a)){
    //This prints the final answer
    tfidf_final(i)
  }
}

// COMMAND ----------


