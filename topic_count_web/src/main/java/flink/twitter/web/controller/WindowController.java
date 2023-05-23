package flink.twitter.web.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/windows")
public class WindowController {

    @GetMapping("/windows")
    public String findAllWindows() {
        return "home";
    }
}