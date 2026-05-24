import os
import sys
import shutil

def CpCert(path = None):
    cert_install_path = None
    if sys.platform == "linux" or sys.platform == "linux2":
        home_path = os.path.expandvars('$HOME')
        cert_install_path = os.path.join(home_path, 'mdga_file')
    elif sys.platform == 'win32':
        c_drive = os.getenv("SYSTEMDRIVE") + '\\'
        documents_path = os.path.join(c_drive, 'Users', 'Public', 'Documents')
        cert_install_path = os.path.join(documents_path, 'mdga_file')

    if not path is None:
        cert_install_path = path

    if not cert_install_path is None:
        if not os.path.exists(cert_install_path):
            os.makedirs(cert_install_path)
        if os.path.exists(cert_install_path):
            wheel_cert_path =  os.path.join(os.path.dirname( os.path.abspath(__file__)), 'cert', '.ca.crt')
            if os.path.exists(wheel_cert_path):
                shutil.copy2(wheel_cert_path, os.path.join(cert_install_path, '.ca.crt'))
            else:
                print('there is no {}'.format(wheel_cert_path))

if __name__ == '__main__':
    argv_num = len(sys.argv)
    path = None
    if argv_num == 2:
        path = sys.argv[1]
    CpCert(path)
