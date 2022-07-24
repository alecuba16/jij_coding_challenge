import spark.tasks.Task_1;
import spark.tasks.Task_2;
import spark.tasks.Task_3;
import org.apache.spark.sql.SparkSession;

public class MainLocalSpark {

    public void run(String[] args) throws Exception {
        String master = "local[*]";
        // Initialises a Spark context.
        SparkSession sparkSession = SparkSession
                .builder()
                .master(master)
                .appName("Adevinta Coding challenge")
                .getOrCreate();

        long startTime = System.nanoTime();
        if (args[0].equals("-task_1")) {
            String[] inputPathTask_1 = {"src/main/resources/siteads"};
            Task_1.run(sparkSession, inputPathTask_1, "src/main/resources/spark_results/task-1");
        }else if (args[0].equals("-task_2")) {
            String[] inputPathTask_2 = {"src/main/resources/ads","src/main/resources/siteads"};
            Task_2.run(sparkSession, inputPathTask_2, "src/main/resources/spark_results/task-2");
        }
        else if (args[0].equals("-task_3")) {
            String[] inputPathTask_3 = {"src/main/resources/sites","src/main/resources/siteads"};
            Task_3.run(sparkSession, inputPathTask_3, "src/main/resources/spark_results/task-3");
        }

        long endTime   = System.nanoTime();
        double totalTime = (endTime - startTime) / 1000000000.0;
        System.out.println(String.format("Spark running time for %s is %.4f seconds",args[0],totalTime));
    }

    public static void main(String[] args) throws Exception {
        new MainLocalSpark().run(args);
    }

}
