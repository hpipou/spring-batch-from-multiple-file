package com.example.demo.controller;

import com.example.demo.batchconfig.ItemProcessorTwo;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    private ItemProcessorTwo itemProcessorAnalytics;

    @GetMapping("/start")
    public BatchStatus load() throws Exception{
        Map<String, JobParameter> params =new HashMap<>();
        params.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters jobParameters=new JobParameters(params);
        JobExecution jobExecution=jobLauncher.run(job,jobParameters);
        while (jobExecution.isRunning()){
            System.out.println("Executions....");
        }
        return jobExecution.getStatus();
    }

    @GetMapping("/total")
    public Map<String,Double> analytics() throws Exception{
        Map<String, Double> map =new HashMap<>();
        map.put("totalCredit", itemProcessorAnalytics.getTotalCredit());
        map.put("totalDebit", itemProcessorAnalytics.getTotalDebit());
        return map;
    }


















}
