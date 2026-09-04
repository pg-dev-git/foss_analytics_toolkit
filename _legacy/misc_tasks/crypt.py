from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
from misc_tasks.terminal_colors import *

def key_encode(_key):
    _key = _key.encode()
    salt = b'SALT'
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=214322,
        backend=default_backend()
        )
    _key = base64.urlsafe_b64encode(kdf.derive(_key))
    return _key

def encrypt_web_auth(_key):
    fernet = Fernet(_key)

    with open('web_sfdc_auth.ini', 'rb') as configfile:
        config_crypt = configfile.read()

    try:
        config_crypt = fernet.encrypt(config_crypt)
    except:
        prRed("You entered the wrong password. Re-launch the tool and try again...")
        line_print()
        quit()

    with open('web_sfdc_auth.ini', 'wb') as encrypted_file:
        encrypted_file.write(config_crypt)



def decrypt_web_auth(_key):
    fernet = Fernet(_key)

    with open('web_sfdc_auth.ini', 'rb') as encrypted_ini:
        encrypted = encrypted_ini.read()

    try:
        decrypt = fernet.decrypt(encrypted)
        decrypt = decrypt.decode()
    except:
        prRed("You entered the wrong password. Re-launch the tool and try again...")
        line_print()
        quit()

    with open('web_sfdc_auth.ini', 'w') as configfile:
        configfile.write(decrypt)

def encrypt_app_auth(_key):
    fernet = Fernet(_key)

    with open('sfdc_auth.ini', 'rb') as configfile:
        config_crypt = configfile.read()

    try:
        config_crypt = fernet.encrypt(config_crypt)
    except:
        prRed("You entered the wrong password. Re-launch the tool and try again...")
        line_print()
        quit()

    with open('sfdc_auth.ini', 'wb') as encrypted_file:
        encrypted_file.write(config_crypt)

def decrypt_app_auth(_key):
    fernet = Fernet(_key)

    with open('sfdc_auth.ini', 'rb') as encrypted_ini:
        encrypted = encrypted_ini.read()

    try:
        decrypt = fernet.decrypt(encrypted)
        decrypt = decrypt.decode()
    except:
        prRed("You entered the wrong password. Re-launch the tool and try again...")
        line_print()
        quit()

    with open('sfdc_auth.ini', 'w') as configfile:
        configfile.write(decrypt)
