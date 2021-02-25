package com.example.batchprocessing.np;

import com.example.batchprocessing.np.common.StudentRowProcessor;
import com.example.batchprocessing.np.common.LoggingStudentWriter;
import com.example.batchprocessing.np.student.HeaderLineHandler;
import com.sun.istack.internal.NotNull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

/**
 * @author Nagaraja
 */
@Configuration
public class StudentJobConfig {

    private static final String PROPERTY_REST_API_URL = "rest.api.to.database.job.api.url";

    @Autowired
    RestTemplate restTemplate;

    @Bean
    FlatFileItemReader<Object> readData(Environment environment, RestTemplate restTemplate) {
        FlatFileItemReader itemReader = new CustomReader(environment.getRequiredProperty(PROPERTY_REST_API_URL), restTemplate);
        itemReader.setLineMapper(getLineMapper());
        itemReader.setLinesToSkip(1);
        itemReader.setSkippedLinesCallback(headerLineHandler());
        return itemReader;
    }

    @NotNull
    private DefaultLineMapper getLineMapper() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        PassThroughFieldSetMapper fieldSetMapper = new PassThroughFieldSetMapper();
        DefaultLineMapper lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(delimitedLineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }


    @Bean
    HeaderLineHandler headerLineHandler() {
        return new HeaderLineHandler();
    }

    @Bean
    ItemProcessor<Object, Object> studentRowProcessor() {
        return new StudentRowProcessor();
    }

    @Bean
    ItemWriter<Object> StudentRowDataWriter() {
        return new LoggingStudentWriter();
    }

    @Bean
    Step restStudentSteps(FlatFileItemReader<Object> readData,
                          ItemProcessor<Object, Object> studentProcessor,
                          ItemWriter<Object> studentDataWriter,
                          HeaderLineHandler headerLineHandler,
                          StepBuilderFactory stepBuilderFactory) {
        SimpleStepBuilder<Object, Object> restStudentStep = stepBuilderFactory.get("restStudentStep")
                .<Object, Object>chunk(100)
                .reader(readData);
        TaskletStep taskletStep = restStudentStep
                .processor(studentProcessor)
                .writer(studentDataWriter)
                .build();
        taskletStep.registerStepExecutionListener(headerLineHandler);
        return taskletStep;
    }


    @Bean
    Job restStudentJobs(JobBuilderFactory jobBuilderFactory,
                        @Qualifier("restStudentSteps") Step restStudentStep) {
        return jobBuilderFactory.get("restStudentJobs")
                .incrementer(new RunIdIncrementer())
                .flow(restStudentStep)
                .end()
                .build();
    }
}
