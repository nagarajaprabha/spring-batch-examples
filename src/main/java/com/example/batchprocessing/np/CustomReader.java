package com.example.batchprocessing.np;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

public class CustomReader extends FlatFileItemReader implements StepExecutionListener {
    byte[] inputData;
    private final String apiUrl;

    private final RestTemplate restTemplate;

    public CustomReader(String apiUrl, RestTemplate restTemplate) {
        this.apiUrl = apiUrl;
        this.restTemplate = restTemplate;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        ResponseEntity<String> response = restTemplate.getForEntity(apiUrl, String.class);
        inputData = response.getBody().getBytes(Charset.defaultCharset());

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
    protected void doOpen() throws Exception {
        /*if(inputData == null) {
            ResponseEntity<String> response = restTemplate.getForEntity(apiUrl, String.class);
            inputData = response.getBody().getBytes(Charset.defaultCharset());
        }*/

        InputStreamResource res = new InputStreamResource(new ByteArrayInputStream(inputData));
        setResource(res);
        super.doOpen();
    }
    @Override
    public void setResource(Resource resource) {

        // Convert byte array to input stream
        InputStream is = null;
        is = new ByteArrayInputStream(inputData);

        // Create springbatch input stream resource
        InputStreamResource res = new InputStreamResource(is);

        // Set resource
        super.setResource(res);
    }


}
