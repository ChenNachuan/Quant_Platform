import platform
import os
import sys
dir_tmp, file_name = os.path.split(os.path.realpath(__file__))
if os.path.exists(os.path.join(dir_tmp, 'cert_install.py')):
    from .cert_install import CpCert
    try:
        CpCert()
    except Exception as e:
        print('waring: {}'.format(e))
def GetOSBits():
    os_name = os.name.lower()
    os_bits = 64
    if os_name == 'nt' and sys.version_info[:2] <(2,7):
        machine_info = os.environ.get("PROCESSOR_ARCHITEW6432", os.environ.get('PROCESSOR_ARCHITECTURE',''))
    else:
        machine_info = platform.machine()
    if machine_info == 'AMD64':
        os_bits = 64
    elif machine_info == 'x86_64':
        os_bits = 64
    elif machine_info == 'i386':
        os_bits = 32
    elif machine_info == 'x86':
        os_bits = 32
    return os_bits


def GetOsInfo():
    os_info = 'linux'
    if 'win' in sys.platform:
        os_info = 'win'
    elif 'linux' in sys.platform:
        os_info = 'linux'
    else:
        raise Exception('this system is not supported')
    return os_info


os_bits = GetOSBits()
os_info = GetOsInfo()

if os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.10"):
    from .linux_py310_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.11"):
    from .linux_py311_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.12"):
    from .linux_py312_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.13"):
    from .linux_py313_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.14"):
    from .linux_py314_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.6"):
    from .linux_py36_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.8"):
    from .linux_py38_x64_package.tgw import *
elif os_info == 'linux' and os_bits == 64 and sys.version.startswith("3.9"):
    from .linux_py39_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.10"):
    from .win_py310_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.11"):
    from .win_py311_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.12"):
    from .win_py312_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.13"):
    from .win_py313_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.14"):
    from .win_py314_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.6"):
    from .win_py36_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.8"):
    from .win_py38_x64_package.tgw import *
elif os_info == 'win' and os_bits == 64 and sys.version.startswith("3.9"):
    from .win_py39_x64_package.tgw import *
else:
    raise Exception('this system is not supported. <Info: os:{} bit:{} python:{}>'.format(os_info, os_bits, sys.version))

if os.path.exists(os.path.join(dir_tmp, 'server_spi.py')):
    from .server_spi import *
if os.path.exists(os.path.join(dir_tmp, 'interface.py')):
    from .interface import *
if os.path.exists(os.path.join(dir_tmp, 'tmp_spi.py')):
    from .tmp_spi import *
if os.path.exists(os.path.join(dir_tmp, 'error_code.py')):
    from .error_code import *
if os.path.exists(os.path.join(dir_tmp, 'base_struct.py')):
    from .base_struct import *
