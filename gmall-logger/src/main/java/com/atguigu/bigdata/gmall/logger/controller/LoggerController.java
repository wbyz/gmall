package com.atguigu.bigdata.gmall.logger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @author Witzel
 * @since 2019/7/19 20:05
 */
@RestController //@Controller + @ResponseBody
public class LoggerController {

//    @RequestMapping(path = "test",method = RequestMethod.GET)
//    @ResponseBody
    @GetMapping("test")     // 若确定是GET请求可以直接写GetMapping()
    public String getTest(){
        System.out.println("!!!!!!!!!!");
        return "success";
    }

}
