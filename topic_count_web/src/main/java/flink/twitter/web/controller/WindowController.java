package flink.twitter.web.controller;

import flink.twitter.web.model.WindowModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/windows")
public class WindowController {

    @Autowired
    private WindowModel model;


    @GetMapping("")
    public String findAllWindows() {
        return model.findAllWindows();
    }
}