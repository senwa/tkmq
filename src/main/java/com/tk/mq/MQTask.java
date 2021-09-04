package com.tk.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileReader;
import java.util.List;

@Component
@Slf4j
public class MQTask implements CommandLineRunner {

    @Value("${tkmq.exchange}")
    private String exChange;
    @Value("${tkmq.routingkey}")
    private String routingkey;
    @Value("${tkmq.messageFilePath}")
    private String messageFilePath;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Override
    public void run(String... args) throws Exception {
        if(StringUtils.isNotEmpty(exChange) && StringUtils.isNotEmpty(routingkey) && StringUtils.isNotEmpty(messageFilePath)){
            //读取文件
            log.info("读取消息文件{}",messageFilePath);
            File messaegeFile = new File(messageFilePath);
            if(messaegeFile.exists() && messaegeFile.isFile() && messaegeFile.canRead()){
                log.info("开始执行MQ任务,exchange={},routingkey={}",exChange,routingkey);
                List<String> messageStringList = FileUtils.readLines(messaegeFile, "UTF-8");
                for (String message : messageStringList) {
                    if(StringUtils.isNotEmpty(message)){
                        rabbitTemplate.convertAndSend(exChange, routingkey, message);
                        log.debug("发送完成:{}",message);
                    }
                }

                log.info("MQ任务执行完成,exchange={},routingkey={}",exChange,routingkey);
            }else{
                log.error("消息数据文件信息错误");
            }

        }else{
            log.error("配置信息错误");
        }


    }
}
