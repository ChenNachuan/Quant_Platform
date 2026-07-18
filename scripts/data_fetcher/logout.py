# -*- coding: utf-8 -*-
"""强制退出 AmazingData 账号"""

import os


def main():
    import AmazingData as ad

    username = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")

    # 先登录再退出，确保释放连接
    ad.login(username=username, password=password, host=host, port=int(port))
    ad.logout(username=username)
    print(f"已退出账号: {username}")


if __name__ == "__main__":
    main()
