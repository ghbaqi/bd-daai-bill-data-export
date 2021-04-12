package com.ppio.data.bill;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    int i;

    @GetMapping("/hi")
    public String hi() {
        return "hi " + i++;
    }


    @GetMapping("/mg")
    public String mg() {
        return "hi " + i++;
    }
}
