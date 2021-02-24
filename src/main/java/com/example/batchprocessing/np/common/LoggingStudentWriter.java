package com.example.batchprocessing.np.common;

import com.example.batchprocessing.np.student.StudentDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This custom {@code ItemWriter} writes the information of the student to
 * the log
 *
 * @author Nagaraja
 */
public class LoggingStudentWriter implements ItemWriter<Object>, StepExecutionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingStudentWriter.class);
    StepExecution stepExecution;
    @Override
    public void write(List<? extends Object> items) throws Exception {
        LOGGER.info("\n \n \t ********************* \t \n ");
        LOGGER.info("Received the information of {} students", items.size());
        LOGGER.info("\n \n \t ######################### \t \n  "+ stepExecution.getExecutionContext().get("headerKey"));
        LOGGER.info("\n \n \t %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \t \n ");
        String header = (String) stepExecution.getExecutionContext().get("headerKey");

        FieldSet fs = new DefaultFieldSet(header.split(","));
        List resultRows = new ArrayList();
        resultRows.add(Collections.singletonList(fs));
        resultRows.addAll(items);
        resultRows.forEach(i -> LOGGER.info("Received the information of a student: {}", i));
    }

    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    public ExitStatus afterStep(StepExecution stepExecution) {
        JobExecution jobExecution = stepExecution.getJobExecution();
        ExecutionContext jobContext = jobExecution.getExecutionContext();
        System.out.println(" Header value " + (String) jobContext.get("headerKey"));
        LOGGER.info("\n \n \t %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \t \n ");
        LOGGER.info("\n \n \t  \t \n "+ "Header value " + (String) stepExecution.getExecutionContext().get("headerKey"));
        LOGGER.info("\n \n \t %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \t \n ");
        return null;
    }
}
