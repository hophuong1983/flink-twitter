package flink.twitter.web.controller;

import flink.twitter.web.model.PerWindowTopicCountModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/topics")
public class PerWindowTopicCountController {

    @Autowired
    private PerWindowTopicCountModel model;


    @GetMapping("")
    public String findAllTopics() {
        return model.findAllTopics();
    }

    @GetMapping("/{topic}/{window}")
    public String getCountPerTopicWindow(@PathVariable String topic, @PathVariable int window) {
        return model.getCountPerTopicWindow(topic, window);
    }

}