package cn.newbeedaly.springbootejob.job;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SpringDataflowJob implements DataflowJob {


  @Override
  public List fetchData(ShardingContext shardingContext) {
    System.out.println("fetchData job..");
    List<String> list = new ArrayList<>();
    list.add("newbeedaly");
    return list;
  }

  @Override
  public void processData(ShardingContext shardingContext, List data) {
    System.out.println("processData job..");
  }

}
