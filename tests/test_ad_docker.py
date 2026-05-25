# -*- coding: utf-8 -*-
"""
测试 AmazingData Docker 环境配置
用于验证 PyCharm Docker Compose 解释器是否配置正确

运行方式:
  1. PyCharm 中直接运行此文件（使用 Docker Compose 解释器）
  2. 命令行: docker-compose run --rm amazingdata python3 /scripts/tests/test_ad_docker.py
"""

import sys
import os


def test_environment():
    """测试环境变量"""
    print("=" * 50)
    print("1. 环境变量检查")
    print("=" * 50)
    print(f"Python 版本: {sys.version}")
    print(f"Python 路径: {sys.executable}")
    print(f"工作目录: {os.getcwd()}")
    print(f"USER: {os.getenv('USER', '未设置')}")
    print(f"HOST: {os.getenv('HOST', '未设置')}")
    print(f"PORT: {os.getenv('PORT', '未设置')}")
    print()


def test_volume_mounts():
    """测试 volume 挂载"""
    print("=" * 50)
    print("2. Volume 挂载检查")
    print("=" * 50)

    # 检查 /data 目录
    if os.path.exists("/data"):
        print("/data 目录: 存在")
        data_contents = os.listdir("/data")
        print(f"  内容: {data_contents[:5]}...")
    else:
        print("/data 目录: 不存在")

    # 检查 /scripts 目录
    if os.path.exists("/scripts"):
        print("/scripts 目录: 存在")
        scripts_contents = os.listdir("/scripts")
        print(f"  内容: {scripts_contents[:5]}...")
    else:
        print("/scripts 目录: 不存在")
    print()


def test_amazingdata_import():
    """测试 AmazingData 导入"""
    print("=" * 50)
    print("3. AmazingData 导入测试")
    print("=" * 50)

    try:
        import AmazingData as ad

        print("AmazingData 导入: 成功")
        print(f"  版本: {getattr(ad, '__version__', '未知')}")
    except ImportError as e:
        print(f"AmazingData 导入: 失败 - {e}")
    print()


def test_amazingdata_login():
    """测试 AmazingData 登录"""
    print("=" * 50)
    print("4. AmazingData 登录测试")
    print("=" * 50)

    try:
        import AmazingData as ad

        username = os.getenv("USER")
        password = os.getenv("PASSWORD")
        host = os.getenv("HOST")
        port = os.getenv("PORT")

        if not all([username, password, host, port]):
            print("登录测试: 跳过 - 环境变量不完整")
            return

        ad.login(username=username, password=password, host=host, port=int(port))
        print("登录: 成功")

        # 测试登出
        ad.logout(username=username)
        print("登出: 成功")

    except Exception as e:
        print(f"登录测试: 失败 - {e}")
    print()


def main():
    print("\n" + "=" * 50)
    print("AmazingData Docker 环境测试")
    print("=" * 50 + "\n")

    test_environment()
    test_volume_mounts()
    test_amazingdata_import()
    test_amazingdata_login()

    print("=" * 50)
    print("测试完成")
    print("=" * 50)


if __name__ == "__main__":
    main()
