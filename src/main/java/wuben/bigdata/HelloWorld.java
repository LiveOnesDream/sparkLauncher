package wuben.bigdata;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: launcher1
 * @description
 * @author: z p、
 * @create: 2020-04-16 09:35
 **/
public class HelloWorld {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        List<Person> persons = new ArrayList<Person>();

        persons.add(new Person("zhangsan", 22, "male"));
        persons.add(new Person("lisi", 25, "male"));
        persons.add(new Person("wangwu", 23, "female"));

        spark.createDataFrame(persons, Person.class).show(false);

        spark.close();
    }
}
