# -*- coding: utf-8 -*-
"""
Protobuf 定义分析工具
从 gRPC 流量捕获中提取 protobuf 消息结构，生成 .proto 文件

使用方式:
    python analyze_protobuf.py --log capture_output/capture_log.jsonl
    python analyze_protobuf.py --pcap capture.pcapng --keys sslkeys.log
"""

import json
import struct
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime


# Protobuf wire types
WIRE_TYPE_VARINT = 0
WIRE_TYPE_64BIT = 1
WIRE_TYPE_LENGTH_DELIMITED = 2
WIRE_TYPE_32BIT = 5


def decode_varint(data, pos):
    """解码 protobuf varint"""
    result = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        if (byte & 0x80) == 0:
            return result, pos
        shift += 7
    raise ValueError("Invalid varint")


def decode_protobuf_field(data, pos):
    """解码单个 protobuf 字段"""
    if pos >= len(data):
        return None

    # 读取 tag (field_number << 3 | wire_type)
    tag, pos = decode_varint(data, pos)
    field_number = tag >> 3
    wire_type = tag & 0x07

    if wire_type == WIRE_TYPE_VARINT:
        value, pos = decode_varint(data, pos)
        return field_number, wire_type, value, pos

    elif wire_type == WIRE_TYPE_64BIT:
        if pos + 8 > len(data):
            return None
        value = struct.unpack('<d', data[pos:pos+8])[0]
        return field_number, wire_type, value, pos + 8

    elif wire_type == WIRE_TYPE_LENGTH_DELIMITED:
        length, pos = decode_varint(data, pos)
        if pos + length > len(data):
            return None
        value = data[pos:pos+length]
        return field_number, wire_type, value, pos + length

    elif wire_type == WIRE_TYPE_32BIT:
        if pos + 4 > len(data):
            return None
        value = struct.unpack('<f', data[pos:pos+4])[0]
        return field_number, wire_type, value, pos + 4

    else:
        return None


def decode_protobuf_message(data):
    """解码整个 protobuf 消息"""
    fields = []
    pos = 0

    while pos < len(data):
        result = decode_protobuf_field(data, pos)
        if result is None:
            break
        field_number, wire_type, value, pos = result
        fields.append((field_number, wire_type, value))

    return fields


def guess_proto_type(wire_type, value):
    """猜测 protobuf 类型"""
    if wire_type == WIRE_TYPE_VARINT:
        if isinstance(value, bool):
            return "bool"
        elif value < 0:
            return "int64"  # sint64 或 int64
        elif value < 2**31:
            return "int32"
        else:
            return "int64"

    elif wire_type == WIRE_TYPE_64BIT:
        return "double"

    elif wire_type == WIRE_TYPE_32BIT:
        return "float"

    elif wire_type == WIRE_TYPE_LENGTH_DELIMITED:
        # 尝试判断是 string 还是 bytes 还是嵌套消息
        try:
            decoded = value.decode('utf-8')
            if all(c.isprintable() or c in '\n\r\t' for c in decoded):
                return "string"
        except:
            pass

        # 尝试解码为嵌套消息
        try:
            nested = decode_protobuf_message(value)
            if nested and all(f[0] > 0 for f in nested):
                return "bytes"  # 可能是嵌套消息，暂标记为 bytes
        except:
            pass

        return "bytes"

    return "unknown"


def analyze_jsonl_log(log_file):
    """分析 JSONL 日志文件，提取数据结构"""
    structures = defaultdict(lambda: {
        'requests': [],
        'responses': [],
        'module': '',
        'function': ''
    })

    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
            except:
                continue

            module = entry.get('module', '')
            function = entry.get('function', '')
            key = f"{module}.{function}"

            structures[key]['module'] = module
            structures[key]['function'] = function

            # 记录请求参数
            if 'args' in entry or 'kwargs' in entry:
                structures[key]['requests'].append({
                    'args': entry.get('args'),
                    'kwargs': entry.get('kwargs')
                })

            # 记录响应结构
            if 'result_columns' in entry:
                structures[key]['responses'].append({
                    'type': 'DataFrame',
                    'columns': entry['result_columns'],
                    'shape': entry.get('result_shape'),
                    'sample': entry.get('result_sample')
                })
            elif 'result_keys' in entry:
                structures[key]['responses'].append({
                    'type': 'dict',
                    'keys': entry['result_keys'],
                    'sample': entry.get('result_sample')
                })
            elif 'result_len' in entry:
                structures[key]['responses'].append({
                    'type': 'list',
                    'length': entry['result_len'],
                    'sample': entry.get('result_sample')
                })

    return structures


def generate_proto_from_structures(structures):
    """从分析的结构生成 .proto 文件"""
    lines = []
    lines.append('// Auto-generated from gRPC traffic capture')
    lines.append(f'// Generated at: {datetime.now().isoformat()}')
    lines.append('// Note: Field numbers are estimated, types may need adjustment')
    lines.append('')
    lines.append('syntax = "proto3";')
    lines.append('')
    lines.append('package galaxy.tgw;')
    lines.append('')

    # 收集所有消息类型
    messages = {}
    enums = {}

    for key, data in structures.items():
        module = data['module']
        function = data['function']

        # 为每个函数生成请求和响应消息
        req_msg_name = f"Req{function.replace('get_', '').replace('query_', '').title()}"
        rsp_msg_name = f"Rsp{function.replace('get_', '').replace('query_', '').title()}"

        # 分析请求参数
        if data['requests']:
            req = data['requests'][0]
            req_fields = []
            field_num = 1

            if req.get('args'):
                for i, arg in enumerate(req['args']):
                    if isinstance(arg, list):
                        req_fields.append(f"  repeated string arg{i} = {field_num};")
                    elif isinstance(arg, int):
                        req_fields.append(f"  int32 arg{i} = {field_num};")
                    elif isinstance(arg, str):
                        req_fields.append(f"  string arg{i} = {field_num};")
                    field_num += 1

            if req.get('kwargs'):
                for name, value in req['kwargs'].items():
                    if isinstance(value, list):
                        req_fields.append(f"  repeated string {name} = {field_num};")
                    elif isinstance(value, int):
                        req_fields.append(f"  int32 {name} = {field_num};")
                    elif isinstance(value, str):
                        req_fields.append(f"  string {name} = {field_num};")
                    elif isinstance(value, float):
                        req_fields.append(f"  double {name} = {field_num};")
                    elif isinstance(value, bool):
                        req_fields.append(f"  bool {name} = {field_num};")
                    field_num += 1

            if req_fields:
                messages[req_msg_name] = req_fields

        # 分析响应结构
        if data['responses']:
            rsp = data['responses'][0]
            rsp_fields = []
            field_num = 1

            if rsp['type'] == 'DataFrame' and rsp.get('columns'):
                # DataFrame 的每一列可能是一个 repeated 字段或嵌套消息
                for col in rsp['columns']:
                    col_name = col.lower().replace(' ', '_')
                    rsp_fields.append(f"  repeated string {col_name} = {field_num};")
                    field_num += 1

            elif rsp['type'] == 'dict' and rsp.get('keys'):
                for key_name in rsp['keys']:
                    rsp_fields.append(f"  string {key_name} = {field_num};")
                    field_num += 1

            if rsp_fields:
                messages[rsp_msg_name] = rsp_fields

    # 生成 service 定义
    lines.append('service IGMDApi {')
    for key, data in structures.items():
        function = data['function']
        func_name = function.replace('get_', 'Get').replace('query_', 'Query')
        msg_name = func_name.title().replace('_', '')
        lines.append(f'  rpc {msg_name}(Req{msg_name}) returns (Rsp{msg_name}) {{}}')
    lines.append('}')
    lines.append('')

    # 生成 message 定义
    for msg_name, fields in messages.items():
        lines.append(f'message {msg_name} {{')
        lines.extend(fields)
        lines.append('}')
        lines.append('')

    return '\n'.join(lines)


def generate_api_summary(structures):
    """生成 API 调用摘要"""
    lines = []
    lines.append("# AmazingData API 调用摘要")
    lines.append(f"# Generated at: {datetime.now().isoformat()}")
    lines.append("")
    lines.append("=" * 70)

    for key, data in sorted(structures.items()):
        lines.append(f"\n## {key}")
        lines.append(f"Module: {data['module']}")
        lines.append(f"Function: {data['function']}")

        if data['requests']:
            req = data['requests'][0]
            lines.append("\nRequest parameters:")
            if req.get('args'):
                lines.append(f"  args: {json.dumps(req['args'], ensure_ascii=False, indent=4)[:500]}")
            if req.get('kwargs'):
                lines.append(f"  kwargs: {json.dumps(req['kwargs'], ensure_ascii=False, indent=4)[:500]}")

        if data['responses']:
            rsp = data['responses'][0]
            lines.append(f"\nResponse type: {rsp['type']}")
            if rsp.get('columns'):
                lines.append(f"Columns: {rsp['columns'][:20]}")
                if len(rsp['columns']) > 20:
                    lines.append(f"  ... and {len(rsp['columns']) - 20} more")
            if rsp.get('shape'):
                lines.append(f"Shape: {rsp['shape']}")
            if rsp.get('sample'):
                sample_str = json.dumps(rsp['sample'], ensure_ascii=False, indent=2)[:1000]
                lines.append(f"Sample data:\n{sample_str}")

        lines.append("-" * 70)

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Protobuf 定义分析工具')
    parser.add_argument('--log', '-l', help='JSONL 日志文件路径')
    parser.add_argument('--output', '-o', default='proto_output', help='输出目录')

    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)

    if args.log:
        print(f"Analyzing log file: {args.log}")

        # 分析 JSONL 日志
        structures = analyze_jsonl_log(args.log)

        if not structures:
            print("No data structures found in log file.")
            return

        print(f"Found {len(structures)} unique API calls")

        # 生成 API 摘要
        summary = generate_api_summary(structures)
        summary_file = output_dir / "api_summary.md"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary)
        print(f"API summary saved to: {summary_file}")

        # 生成 .proto 文件
        proto_content = generate_proto_from_structures(structures)
        proto_file = output_dir / "amazingdata.proto"
        with open(proto_file, 'w', encoding='utf-8') as f:
            f.write(proto_content)
        print(f"Proto file saved to: {proto_file}")

        # 生成 JSON 格式的结构定义
        struct_json = {}
        for key, data in structures.items():
            struct_json[key] = {
                'module': data['module'],
                'function': data['function'],
                'request_count': len(data['requests']),
                'response_count': len(data['responses']),
                'response_types': list(set(r['type'] for r in data['responses']))
            }

        json_file = output_dir / "structures.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(struct_json, f, indent=2, ensure_ascii=False)
        print(f"Structure JSON saved to: {json_file}")

    else:
        print("Please specify --log file path")
        print("Example: python analyze_protobuf.py --log capture_output/capture_log.jsonl")


if __name__ == '__main__':
    main()
