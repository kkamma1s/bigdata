import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public final class test {

    public static int year(String date) {
        return Integer.parseInt(date.substring(date.length() - 4, date.length()));
    }

    public static void main(String args[]) {
        SparkSession sc = SparkSession.builder().appName("apache").config("spark.master", "local").getOrCreate();
        //Dataset<Row> json_dataset = sc.read().json("/Users/nikilreddy/Desktop/books_reviews.json");
        //JavaPairRDD<String, String> data = json_dataset.javaRDD().mapToPair(row ->
        //new Tuple2<>(row.getString(1), row.getString(9)));
        //JavaPairRDD<String, Integer> data1 = data.mapValues(date -> year(date));
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> json_dataset = sqlContext.read().format("json").json("/Users/nikilreddy/Desktop/books_reviews.json");
        json_dataset.createOrReplaceTempView("plotting");
        Dataset<Row> json_dataset1 = sqlContext.sql("select asin,reviewTime from plotting");
        JavaRDD<Row> json_dataset2 = json_dataset1.javaRDD();

        JavaPairRDD<String, String> json_dataset3 = json_dataset2.mapToPair(row ->
                new Tuple2<>(row.getString(0), row.getString(1)));
        JavaPairRDD<String, Integer> json_dataset4 = json_dataset3.mapValues(apache::year);
        JavaRDD<Integer> json_dataset5 = json_dataset4.values();
        // json_dataset5.saveAsTextFile("/Users/nikilreddy/Desktop/report.txt");
        int sum = json_dataset5.reduce((a,b)->a+b);

        System.out.println("Sum of numbers is : "+sum);
        Double d = Math.sqrt(sum);
        //json_dataset4.saveAsTextFile("/Users/nikilreddy/Desktop/report.txt");


    }
}
