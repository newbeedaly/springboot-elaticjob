package cn.newbeedaly.springbootejob.config;

import cn.newbeedaly.springbootejob.job.SpringDataflowJob;
import cn.newbeedaly.springbootejob.job.SpringSimpleJob;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticJobConfig {

  @Autowired
  private ZookeeperRegistryCenter regCenter;
  /**
   * 配置任务监听器
   * @return
   */
  @Bean
  public ElasticJobListener elasticJobListener() {
    return new MyElasticJobListener();
  }
  /**
   * 配置任务详细信息
   * @param jobClass
   * @param cron
   * @param shardingTotalCount
   * @param shardingItemParameters
   * @return
   */
  private LiteJobConfiguration getLiteJobConfiguration(final Class<? extends SimpleJob> jobClass,
                                                       final String cron,
                                                       final int shardingTotalCount,
                                                       final String shardingItemParameters) {
    return LiteJobConfiguration.newBuilder(new SimpleJobConfiguration(
        JobCoreConfiguration.newBuilder(jobClass.getName(), cron, shardingTotalCount)
            .shardingItemParameters(shardingItemParameters).build()
        , jobClass.getCanonicalName())
    ).overwrite(true).build();
  }

  private LiteJobConfiguration getDataFlowJobConfiguration(final Class<? extends DataflowJob> jobClass,
                                                       final String cron,
                                                       final int shardingTotalCount,
                                                       final String shardingItemParameters) {
    return LiteJobConfiguration.newBuilder(new DataflowJobConfiguration(
        JobCoreConfiguration.newBuilder(jobClass.getName(), cron, shardingTotalCount)
            .shardingItemParameters(shardingItemParameters).build()
        , jobClass.getCanonicalName(),false)
    ).overwrite(true).build();
  }

  @Bean(initMethod = "init")
  public JobScheduler simpleJobScheduler(final SpringSimpleJob simpleJob,
                                         @Value("${simpleJob.cron}") final String cron,
                                         @Value("${simpleJob.shardingTotalCount}") final int shardingTotalCount,
                                         @Value("${simpleJob.shardingItemParameters}") final String shardingItemParameters) {
    MyElasticJobListener elasticJobListener = new MyElasticJobListener();
    return new SpringJobScheduler(simpleJob, regCenter,
        getLiteJobConfiguration(simpleJob.getClass(), cron, shardingTotalCount, shardingItemParameters),
        elasticJobListener);
  }

  @Bean(initMethod = "init")
  public JobScheduler dataFlowJobScheduler(final SpringDataflowJob dataFlowJob,
                                         @Value("${dataFlowJob.cron}") final String cron,
                                         @Value("${dataFlowJob.shardingTotalCount}") final int shardingTotalCount,
                                         @Value("${dataFlowJob.shardingItemParameters}") final String shardingItemParameters) {
    MyElasticJobListener elasticJobListener = new MyElasticJobListener();
    return new SpringJobScheduler(dataFlowJob, regCenter,
        getDataFlowJobConfiguration(dataFlowJob.getClass(), cron, shardingTotalCount, shardingItemParameters),
        elasticJobListener);
  }
}
