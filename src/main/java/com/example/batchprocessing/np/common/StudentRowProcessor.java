package com.example.batchprocessing.np.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.transform.DefaultFieldSet;

/**
 * This custom {@code ItemProcessor} simply writes the information of the
 * processed student to the log and returns the processed object.
 *
 * @author Nagaraja
 */
public class StudentRowProcessor implements ItemProcessor<Object, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudentRowProcessor.class);

    @Override
    public Object process(Object item) throws Exception {
        if(item instanceof DefaultFieldSet){
            LOGGER.info("!!!!!!!!!!!!!!!!!@@#######@", (((DefaultFieldSet) item).getValues().toString()));
        }
        LOGGER.info("Processing student information: {}", item);
        LOGGER.info("Processing student information: {}", item);
        return item;
    }
}
