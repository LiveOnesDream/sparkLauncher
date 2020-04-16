package wuben.bigdata;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * JAVA 程序中提交spark 任务
 * 1.将本项目打jar包
 * 2.spark-launcher_2.11-2.3.0.jar（根据spark版本选择）
 * 3.把两个jar包放在spark 任意一个节点上，启动本项目jar
 * @program: launcher1
 * @description
 * @author: z p、
 * @create: 2020-04-15 17:40
 **/
public class launcherTest {

    static Logger logger = LoggerFactory.getLogger(launcherTest.class);

    public static void main(String[] args) throws IOException {
        HashMap env = new HashMap();
        env.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");
        env.put("JAVA_HOME", "/usr/java/jdk1.8.0_201");
        SparkAppHandle handler = new SparkLauncher(env)
                .setAppName("hello-world")
                .setSparkHome("/opt/cloudera/parcels/SPARK2/lib/spark2")
                .setAppResource("/usr/local/spark/launcher-1.0-SNAPSHOT.jar")
                .setMainClass("wuben.bigdata.HelloWorld")
                .setMaster("yarn")
                .setDeployMode("client")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.cores", "3")
                // .setConf("spark.akka.frameSize", "200")
                // .setConf("spark.executor.instances", "32")
                // .setConf("spark.default.parallelism", "10")
                // .setConf("spark.driver.allowMultipleContexts", "true")
                .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        logger.info("state:" + sparkAppHandle.getState().toString());
                    }

                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        logger.info("Info:" + sparkAppHandle.getState().toString());
                    }
                });
        System.out.println("The task is executing, please wait ....");
        while (!"FINISHED".equalsIgnoreCase(handler.getState().toString()) &&
                !"FAILED".equalsIgnoreCase(handler.getState().toString())) {

            logger.info("id    " + handler.getAppId());
            logger.info("state " + handler.getState());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
