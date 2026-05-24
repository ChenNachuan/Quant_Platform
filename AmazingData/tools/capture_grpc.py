# -*- coding: utf-8 -*-
"""
gRPC 流量捕获脚本
在 tgw Python 层拦截所有调用，记录请求参数和响应数据结构

使用方式 (Windows):
    python -m WealthManager.tools.capture_grpc

输出:
    - capture_log.jsonl: 所有调用的 JSON 日志
    - capture_structures.txt: 数据结构摘要
"""

import json
import time
import traceback
import functools
from pathlib import Path
from datetime import datetime

# 输出目录
OUTPUT_DIR = Path(__file__).parent / "capture_output"
OUTPUT_DIR.mkdir(exist_ok=True)

LOG_FILE = OUTPUT_DIR / "capture_log.jsonl"
STRUCT_FILE = OUTPUT_DIR / "capture_structures.txt"


def serialize_for_json(obj, depth=0, max_depth=5):
    """将对象序列化为 JSON 可存储的格式"""
    if depth > max_depth:
        return f"<max_depth:{type(obj).__name__}>"

    if obj is None:
        return None
    elif isinstance(obj, (bool, int, float, str)):
        return obj
    elif isinstance(obj, bytes):
        try:
            return obj.decode('utf-8')
        except:
            return f"<bytes:{len(obj)}>"
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item, depth + 1, max_depth) for item in obj[:100]]  # 限制100条
    elif isinstance(obj, dict):
        return {str(k): serialize_for_json(v, depth + 1, max_depth) for k, v in list(obj.items())[:50]}
    elif hasattr(obj, '__dict__'):
        return {
            '_type': type(obj).__name__,
            **{k: serialize_for_json(v, depth + 1, max_depth) for k, v in vars(obj).items() if not k.startswith('_')}
        }
    elif hasattr(obj, 'to_dict'):
        return serialize_for_json(obj.to_dict(), depth + 1, max_depth)
    else:
        return f"<{type(obj).__name__}>"


class CallCapture:
    """捕获所有 tgw 调用"""

    def __init__(self):
        self.log_file = open(LOG_FILE, 'a', encoding='utf-8')
        self.structures = set()

    def log_call(self, module, func_name, args, kwargs, result=None, error=None, duration_ms=0):
        """记录一次调用"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'module': module,
            'function': func_name,
            'args': serialize_for_json(args),
            'kwargs': serialize_for_json(kwargs),
            'duration_ms': duration_ms,
        }

        if error:
            entry['error'] = str(error)
            entry['error_type'] = type(error).__name__
        elif result is not None:
            entry['result_type'] = type(result).__name__
            if hasattr(result, 'shape'):  # DataFrame
                entry['result_shape'] = list(result.shape)
                entry['result_columns'] = list(result.columns) if hasattr(result, 'columns') else []
                entry['result_sample'] = serialize_for_json(result.head(3).to_dict()) if not result.empty else []
            elif isinstance(result, dict):
                entry['result_keys'] = list(result.keys())
                entry['result_sample'] = serialize_for_json({k: v for k, v in list(result.items())[:3]})
            elif isinstance(result, (list, tuple)):
                entry['result_len'] = len(result)
                entry['result_sample'] = serialize_for_json(result[:3])
            else:
                entry['result'] = serialize_for_json(result)

        self.log_file.write(json.dumps(entry, ensure_ascii=False) + '\n')
        self.log_file.flush()

        # 记录数据结构
        struct_key = f"{module}.{func_name}"
        if struct_key not in self.structures:
            self.structures.add(struct_key)
            with open(STRUCT_FILE, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"[{entry['timestamp']}] {struct_key}\n")
                f.write(f"Args: {json.dumps(entry['args'], ensure_ascii=False, indent=2)}\n")
                if 'result_columns' in entry:
                    f.write(f"Result columns: {entry['result_columns']}\n")
                if 'result_sample' in entry:
                    f.write(f"Result sample: {json.dumps(entry['result_sample'], ensure_ascii=False, indent=2)[:2000]}\n")

    def close(self):
        self.log_file.close()


# 全局捕获器
_capturer = CallCapture()


def capture_wrapper(module_name):
    """创建一个装饰器，捕获模块中所有函数的调用"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = (time.time() - start) * 1000
                _capturer.log_call(module_name, func.__name__, args, kwargs, result=result, duration_ms=duration)
                return result
            except Exception as e:
                duration = (time.time() - start) * 1000
                _capturer.log_call(module_name, func.__name__, args, kwargs, error=e, duration_ms=duration)
                raise
        return wrapper
    return decorator


def wrap_module(module, module_name):
    """包装模块中所有公开函数"""
    for attr_name in dir(module):
        if attr_name.startswith('_'):
            continue
        attr = getattr(module, attr_name)
        if callable(attr) and not isinstance(attr, type):
            wrapped = capture_wrapper(module_name)(attr)
            setattr(module, attr_name, wrapped)


def main():
    """主函数：加载 AmazingData 并运行测试查询"""
    print("=" * 60)
    print("AmazingData gRPC 流量捕获工具")
    print("=" * 60)
    print(f"日志文件: {LOG_FILE}")
    print(f"结构文件: {STRUCT_FILE}")
    print()

    try:
        import tgw
        import AmazingData as ad
    except ImportError as e:
        print(f"导入失败: {e}")
        print("请确保已安装 AmazingData 和 tgw")
        return

    # 包装 tgw interface 模块
    try:
        from tgw import interface as tgw_interface
        wrap_module(tgw_interface, 'tgw.interface')
        print("[OK] 已包装 tgw.interface")
    except Exception as e:
        print(f"[WARN] 包装 tgw.interface 失败: {e}")

    # 包装 AmazingData 模块
    try:
        wrap_module(ad, 'AmazingData')
        print("[OK] 已包装 AmazingData")
    except Exception as e:
        print(f"[WARN] 包装 AmazingData 失败: {e}")

    print()
    print("请手动执行以下操作来捕获流量:")
    print()
    print("  import AmazingData as ad")
    print("  ad.login('username', 'password', 'host', 'port')")
    print()
    print("  # 测试基础数据")
    print("  base = ad.BaseData()")
    print("  print(base.get_code_list('EXTRA_STOCK_A'))")
    print("  print(base.get_code_info('EXTRA_STOCK_A'))")
    print("  print(base.get_calendar())")
    print()
    print("  # 测试行情数据")
    print("  calendar = base.get_calendar()")
    print("  md = ad.MarketData(calendar)")
    print("  print(md.query_kline(['000001.SZ'], 20240101, 20240131, period=ad.constant.Period.day.value))")
    print()
    print("  # 测试信息数据")
    print("  info = ad.InfoData()")
    print("  print(info.get_stock_basic([]))")
    print()
    print("调用完成后，检查以下文件:")
    print(f"  - {LOG_FILE}")
    print(f"  - {STRUCT_FILE}")

    # 启动交互式 Python
    import code
    code.interact(local=locals())


if __name__ == '__main__':
    main()
