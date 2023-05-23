package flink.twitter.web.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/topics")
public class PerWindowTopicCountController {

    @GetMapping("/")
    public String findAllTopics() {
        return "home";
    }

    @GetMapping("/{topic}/{window}")
    public String getCountPerTopicWindow(@PathVariable String topic, @PathVariable int window) {
        return "home";
    }

}