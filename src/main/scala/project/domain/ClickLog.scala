package project.domain

/**
 * 清洗后的日志信息
 * @param ip 日志访问的IP地址
 * @param time  日志访问的时间
 * @param courseId  日志访问的实战课程编号
 * @param statusCode  日志访问的状态码
 * @param referer 日志访问的网页链接
 */
case class ClickLog (ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
