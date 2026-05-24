import tgw
def GetErrorMsg(error_code):
    error_msg_dict = {
        tgw.ErrorCode.kFailure : "失败",
        tgw.ErrorCode.kUnInited : "未初始化",
        tgw.ErrorCode.kNullSpi : "空指针",
        tgw.ErrorCode.kParamIllegal : "参数非法",
        tgw.ErrorCode.kNetError : "网络异常",
        tgw.ErrorCode.kPermissionError : "数据无权限",
        tgw.ErrorCode.kLogonFailed : "未登录",
        tgw.ErrorCode.kAllocateMemoryFailed : "分配内存失败",
        tgw.ErrorCode.kChannelError : "通道错误",
        tgw.ErrorCode.kOverLoad : "查询服务端hqs任务队列溢出",
        tgw.ErrorCode.kLogoned : "账号已登录",
        tgw.ErrorCode.kHqsError : "查询服务端HQS系统错误",
        tgw.ErrorCode.kDqsError : "查询服务端DQS系统错误",
        tgw.ErrorCode.kNonQueryTimePeriod : "非查询时间段(非查询时间段不支持查询)",
        tgw.ErrorCode.kDbAndCodeTableNoCode : "数据库和代码表中没有指定的代码",
        tgw.ErrorCode.kIllegalMode : "api模式非法",
        tgw.ErrorCode.kThreadBusy : "超过最大可用线程资源",
        tgw.ErrorCode.kParseDataError : "数据解析出错",
        tgw.ErrorCode.kTimeout : "获取数据超时",
        tgw.ErrorCode.kFlowOverLimit : "周流量耗尽",
        tgw.ErrorCode.kCodeTableCacheNotAvailable : "代码表缓存不可用",
        tgw.ErrorCode.kOverMaxSubLimit : "超过最大订阅限制",
        tgw.ErrorCode.kLostConnection : "丢失连接",
        tgw.ErrorCode.kOverMaxQueryLimit : "超过最大查询数（含代码表）",
        tgw.ErrorCode.kFunctionIdNull : "三方资讯查询未设置功能号",
        tgw.ErrorCode.kDataEmpty : "数据为空",
        tgw.ErrorCode.kUserNotExist : "用户不存在",
        tgw.ErrorCode.kVerifyFailure : "账号/密码错误",
        tgw.ErrorCode.kApiInterfaceUsing : "api接口不能同时多次调用",
        tgw.ErrorCode.kTaskIdRepeat : "任务id重复",
        tgw.ErrorCode.kSuccess : "成功"
    }
    if not error_code in error_msg_dict:
        return "unknown error code"
    else:
        return error_msg_dict[error_code]
