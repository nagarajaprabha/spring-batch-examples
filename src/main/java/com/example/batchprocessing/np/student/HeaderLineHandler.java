package com.example.batchprocessing.np.student;

import com.example.batchprocessing.np.common.LoggingStudentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.LineCallbackHandler;

/***
 * @Author Nagaraja
 */
public class HeaderLineHandler implements LineCallbackHandler, StepExecutionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderLineHandler.class);
    StepExecution stepExecution;

    public void handleLine(final String headerLine) {
        LOGGER.info("\n \n \t  \t \n "+ "Header value " +headerLine);
        stepExecution.getExecutionContext().put("headerKey", headerLine);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        // TODO Auto-generated method stub

        JobExecution jobExecution = stepExecution.getJobExecution();
        ExecutionContext jobContext = jobExecution.getExecutionContext();
        System.out.println(" Header value " + (String) jobContext.get("headerKey"));
        LOGGER.info("\n \n \t &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& \t \n ");
        LOGGER.info("\n \n \t  \t \n "+ "Header value " + (String) jobContext.get("headerKey"));
        LOGGER.info("\n \n \t &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& \t \n ");
        return stepExecution.getExitStatus();
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // TODO Auto-generated method stub
        this.stepExecution = stepExecution;
    }
}
