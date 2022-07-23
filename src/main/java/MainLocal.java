import mapreduce.tasks.Task_1;
import mapreduce.tasks.Task_2;
import mapreduce.tasks.Task_3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainLocal extends Configured implements Tool {


    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LocalMapReduce");

        if (args[0].equals("-task_1")) {
            String[] inputPathTask_1 = {"src/main/resources/siteads"};
            Task_1.configureJob(job, inputPathTask_1, "src/main/resources/results/task-1");
        }
        else if (args[0].equals("-task_2")) {
            String[] inputPathTask_2 = {"src/main/resources/ads","src/main/resources/siteads"};
            Task_2.configureJob(job, inputPathTask_2, "src/main/resources/results/task-2");
        }
        else if (args[0].equals("-task_3")) {
            String[] inputPathTask_3 = {"src/main/resources/sites","src/main/resources/siteads"};
            Task_3.configureJob(job, inputPathTask_3, "src/main/resources/results/task-3");
        }

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        
        MainLocal driver = new MainLocal();
        int exitCode = ToolRunner.run(driver,args);
        System.exit(exitCode);
    }

}
