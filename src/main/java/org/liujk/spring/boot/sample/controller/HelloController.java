package org.liujk.spring.boot.sample.controller;


import org.liujk.spring.boot.sample.config.PersonConfig;
import org.liujk.spring.boot.sample.config.PersonCustomConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableConfigurationProperties({PersonConfig.class})
public class HelloController {


    @Value("${my.name}")
    private String name;

    @Value("${my.age}")
    private Integer age;

    @Value("${my.phone}")
    private String phone;

    @Autowired
    private PersonConfig personConfig;

    @Autowired
    private PersonCustomConfig personCustomConfig;

    @RequestMapping("/hello-world")
    public String index() {

        String res1 = "name:" + name
                + " age:" + age
                + " phone:" + phone;

        String res2 = "tom.name:" + personConfig.getName()
                + " tom.age:" + personConfig.getAge()
                + " tom.phone:" + personConfig.getPhone();

        String res3 = "custom.name:" + personCustomConfig.getName()
                + " custom.age:" + personCustomConfig.getAge()
                + " custom.phone:" + personCustomConfig.getPhone();


        return res1 + res2 + res3 + " Greetings from hello world!";
    }

}
