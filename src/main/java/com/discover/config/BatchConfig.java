package com.discover.config;

import com.discover.mapper.CustomerMapper;
import com.discover.model.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;

@Configuration
public class BatchConfig {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private CustomerMapper customerMapper;

    @Bean
    public Job importUserJob(JobRepository jobRepository, Step step1) {
        return new JobBuilder("importCustomerJob", jobRepository)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager)throws Exception {
        return new StepBuilder("step1", jobRepository)
                .<Customer, Customer> chunk(10, transactionManager)
                .reader(dbReader())
                .processor(processor())
                .writer(fileWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemStreamReader<Customer> dbReader()
            throws Exception {
        return (ItemStreamReader<Customer>) itemStreamReader(customerMapper, "select * ",
                "from customer",
                "where 1 = 1'");
    }

    @StepScope
    public ItemStreamReader<? extends Object> itemStreamReader(RowMapper rowMapper, String select, String from, String where) throws Exception {
        JdbcPagingItemReader<Object> reader = new JdbcPagingItemReader<Object>();
        reader.setDataSource(dataSource);
        final SqlPagingQueryProviderFactoryBean sqlPagingQueryProviderFactoryBean = new SqlPagingQueryProviderFactoryBean();
        sqlPagingQueryProviderFactoryBean.setDataSource(dataSource);
        // sqlPagingQueryProviderFactoryBean.setDataSource(dataSource);
        sqlPagingQueryProviderFactoryBean.setSelectClause(select);
        sqlPagingQueryProviderFactoryBean.setFromClause(from);
        // sqlPagingQueryProviderFactoryBean.setWhereClause(where);
        sqlPagingQueryProviderFactoryBean.setSortKey("id");
        reader.setQueryProvider(sqlPagingQueryProviderFactoryBean.getObject());
        reader.setPageSize(100);
        reader.setRowMapper(rowMapper);
        reader.afterPropertiesSet();
        reader.setSaveState(false);
        return reader;
    }

    @Bean
    public ItemProcessor<Customer, Customer> processor() {
        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(Customer customer) throws Exception {
                return customer;
            }
        };
    }


    /*


     */
    @Bean
    @StepScope
    public FlatFileItemWriter<Customer> fileWriter() {
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<Customer>();
        writer.setResource(new FileSystemResource("data/output.csv"));
        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("id, first_name, last_name, email, phone, gender, age, registered, orders, spent, job, hobbies, is_married");
            }
        });
        DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<Customer>();
        lineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<Customer>();
        fieldExtractor.setNames(new String[]{"id", "first_name", "last_name", "email", "phone", "gender", "age", "registered", "orders", "spent","job","hobbies","is_married"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(lineAggregator);
        return writer;
    }

}
