import json, requests, os, time, sys, base64
from misc_tasks.terminal_colors import *
from cryptography.fernet import Fernet
from misc_tasks.line import *
from misc_tasks.crypt import *
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class sfdc_login():

    def get_platform():
        platforms = {
            'linux1' : 'Linux',
            'linux2' : 'Linux',
            'darwin' : 'OS X',
            'win32' : 'Windows'
        }
        if sys.platform not in platforms:
            return sys.platform

        return platforms[sys.platform]

    def get_token(_key):
        import configparser

        config = configparser.ConfigParser()

        decrypt_app_auth(_key)

        config.read("sfdc_auth.ini")

        encrypt_app_auth(_key)

        client_id = config.get("DEFAULT", "client_id")
        client_secret = config.get("DEFAULT", "client_secret")
        username = config.get("DEFAULT", "username")
        password = config.get("DEFAULT", "password")
        server_id = config.get("DEFAULT", "server_id")
        server_domain = config.get("DEFAULT", "server_domain")

        data = {
          'grant_type': 'password',
          'client_id': '{}'.format(client_id),
          'client_secret': '{}'.format(client_secret),
          'username': '{}'.format(username),
          'password': '{}'.format(password)
        }

        resp = requests.post('https://login.salesforce.com/services/oauth2/token', data=data)

        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)

        access_token = formatted_response.get("access_token")
        return access_token,server_id,server_domain

    def setup_ini(flag,_key):
        import configparser
        import getpass

        client_id = "Enter your client id"
        client_secret = "Enter your client secret"
        username = "Enter your username"
        password = "Enter your password + token"
        server_id = "Enter you server id \"na100\""
        server_domain = "Enter your instance domain"

        if flag == "Y":
            try:
                os.remove("sfdc_auth.ini")
            except:
                pass

        while client_id == "Enter your client id" and client_secret == "Enter your client secret" and username == "Enter your username" and password == "Enter your password + token" and server_id == "Enter you server id \"na100\"" and server_domain == "Enter your instance domain":

            if os.path.exists("sfdc_auth.ini") == False:
                prYellow("\r\n"+ "Running first time configuration of login credentials..." + "\r\n")
                time.sleep(2)
                config = configparser.ConfigParser()
                config['DEFAULT'] = {'client_id': '{}'.format(client_id),
                                     'client_secret': '{}'.format(client_secret),
                                     'username': '{}'.format(username),
                                     'password': '{}'.format(password),
                                     'server_id': '{}'.format(server_id),
                                     'server_domain': '{}'.format(server_domain)}
                with open('sfdc_auth.ini', 'w') as configfile:
                    config.write(configfile)

                encrypt_app_auth(_key)

            elif os.path.exists("sfdc_auth.ini"):
                config = configparser.ConfigParser()

                decrypt_app_auth(_key)

                config.read("sfdc_auth.ini")

                encrypt_app_auth(_key)

                client_id = config.get("DEFAULT", "client_id")
                client_secret = config.get("DEFAULT", "client_secret")
                username = config.get("DEFAULT", "username")
                password = config.get("DEFAULT", "password")
                server_id = config.get("DEFAULT", "server_id")
                server_domain = config.get("DEFAULT", "server_domain")
                if client_id == "Enter your client id" and client_secret == "Enter your client secret" and username == "Enter your username" and password == "Enter your password + token" and server_id == "Enter you server id \"na100\"" and server_domain == "Enter your instance domain":
                    client_id = input("\r\n"+ "Enter your client id: ")
                    client_secret = input("\r\n"+ "Enter your client secret: ")
                    username = input("\r\n"+ "Enter your username: ")
                    password = input("\r\n"+ "Enter your password: ")
                    server_id = input("\r\n"+ "Enter you server id \"na100\": ")
                    server_domain = input("\r\n"+ "Enter you instance domain: ")
                    config['DEFAULT'] = {'client_id': '{}'.format(client_id),
                                         'client_secret': '{}'.format(client_secret),
                                         'username': '{}'.format(username),
                                         'password': '{}'.format(password),
                                         'server_id': '{}'.format(server_id),
                                         'server_domain': '{}'.format(server_domain)}
                    with open('sfdc_auth.ini', 'w') as configfile:
                        config.write(configfile)

                    encrypt_app_auth(_key)

    def web_auth(flag,_key):
        import subprocess
        import configparser

        username = "Enter your username"
        server_id = "Enter you server id \"na100\""
        access_token = "Value_Replace"
        server_domain = "Enter you instance domain name"


        if flag == "Y":
            os.remove("web_sfdc_auth.ini")

        while username == "Enter your username" and server_id == "Enter you server id \"na100\"" and access_token == "Value_Replace" and server_domain == "Enter you instance domain name":

            if os.path.exists("web_sfdc_auth.ini") == False:
                prYellow("\r\n"+ "Running first time configuration of login credentials..." + "\r\n")
                time.sleep(2)
                config = configparser.ConfigParser()
                config['DEFAULT'] = {'username': '{}'.format(username),
                                     'server_id': '{}'.format(server_id),
                                     'access_token': '{}'.format(access_token),
                                     'server_domain': '{}'.format(server_domain)}
                with open('web_sfdc_auth.ini', 'w') as configfile:
                    config.write(configfile)

                fernet = Fernet(_key)

                with open('web_sfdc_auth.ini', 'rb') as configfile:
                    config_crypt = configfile.read()

                config_crypt = fernet.encrypt(config_crypt)

                with open('web_sfdc_auth.ini', 'wb') as encrypted_file:
                    encrypted_file.write(config_crypt)

            elif os.path.exists("web_sfdc_auth.ini"):
                config = configparser.ConfigParser()

                decrypt_web_auth(_key)

                config.read('web_sfdc_auth.ini')

                encrypt_web_auth(_key)

                username = config.get("DEFAULT", "username")
                server_id = config.get("DEFAULT", "server_id")
                server_domain = config.get("DEFAULT", "server_domain")
                access_token = config.get("DEFAULT", "access_token")
                if username == "Enter your username" and server_id == "Enter you server id \"na100\"" and access_token == "Value_Replace" and server_domain == "Enter you instance domain name":
                    username = input("\r\n"+ "Enter your username: ")
                    server_id = input("\r\n"+ "Enter you server id \"na100\": ")
                    server_domain = input("\r\n"+ "Enter you instance domain name: ")
                    config['DEFAULT'] = {'username': '{}'.format(username),
                                         'server_id': '{}'.format(server_id),
                                         'access_token': '{}'.format(access_token),
                                         'server_domain': '{}'.format(server_domain)}
                    with open('web_sfdc_auth.ini', 'w') as configfile:
                        config.write(configfile)

                    with open('web_sfdc_auth.ini', 'rb') as configfile:
                        config_crypt = configfile.read()

                    config_crypt = fernet.encrypt(config_crypt)

                    with open('web_sfdc_auth.ini', 'wb') as encrypted_file:
                        encrypted_file.write(config_crypt)

    def web_get_token(_key):
        import configparser
        import subprocess

        config = configparser.ConfigParser()

        decrypt_web_auth(_key)

        config.read('web_sfdc_auth.ini')

        encrypt_web_auth(_key)

        username = config.get("DEFAULT", "username")
        server_id = config.get("DEFAULT", "server_id")
        server_domain = config.get("DEFAULT", "server_domain")
        access_token = config.get("DEFAULT", "access_token")

        if access_token == "Value_Replace":

            sfdc_login_command = subprocess.run(["sfdx force:auth:web:login -r https://{}.my.salesforce.com".format(server_domain)], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)

            os_ = sfdc_login.get_platform()

            if os_ == "Windows":
                log_credentials = subprocess.run(["sfdx", "force:org:display", "--json", "-u", "{}".format(username)], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)
            else:
                log_credentials = subprocess.run(["sfdx force:org:display --json -u {}".format(username)], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)

            #print(log_credentials.stdout)

            access_token = json.loads(log_credentials.stdout)

            res_json = json.loads(log_credentials.stdout)

            status = res_json.get('status')

            if status == 0:

                access_token = access_token.get("result").get("accessToken")
                config['DEFAULT'] = {'username': '{}'.format(username),'server_id': '{}'.format(server_id),'access_token': '{}'.format(access_token), 'server_domain': '{}'.format(server_domain)}

                with open('web_sfdc_auth.ini', 'w') as configfile:
                    config.write(configfile)

                encrypt_web_auth(_key)

                return access_token, server_id, server_domain

            elif status == 1:
                message_error = res_json.get('message')
                prRed("\r\n" + "{}".format(message_error))
                time.sleep(1)
                prRed("\r\n" + "Quitting now. Reconfigure the app with the correct username on the next launch..." + "\r\n")
                if os.path.exists("web_sfdc_auth.ini"):
                    os.remove("web_sfdc_auth.ini")
                if os.path.exists("auth_method.ini"):
                    os.remove("auth_method.ini")
                time.sleep(1)
                quit()

        elif access_token != "Value_Replace":

            headers = {
                'Authorization': "Bearer {}".format(access_token)
                }
            resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave'.format(server_domain), headers=headers)
            formatted_response = json.loads(resp.text)
            formatted_response_str = json.dumps(formatted_response, indent=2)

            try:
                for x in formatted_response:
                    msg = x["message"]
                    errorCode = x["errorCode"]

                if msg == "Session expired or invalid" or errorCode == "INVALID_SESSION_ID":
                    prRed("\r\n" + "Token expired. The Web Login process will run now to generate a new one." + "\r\n")
                    time.sleep(5)
                    access_token = "INVALID_TOKEN"
                    return access_token,server_id,server_domain

            except:
                return access_token,server_id,server_domain

    def auth_check(flag):

        import configparser

        auth_method = "999"
        _key = 1

        if flag == "Y":
            os.remove("auth_method.ini")

        while auth_method == "999":
            if os.path.exists("auth_method.ini") == False:
                config = configparser.ConfigParser()
                config['DEFAULT'] = {'method': '{}'.format(auth_method)}
                with open('auth_method.ini', 'w') as configfile:
                    config.write(configfile)
            elif os.path.exists("auth_method.ini"):
                config = configparser.ConfigParser()
                config.read("auth_method.ini")
                auth_method = config.get("DEFAULT", "method")
                if auth_method == "999":
                    key = 1
                    key2 = 2
                    while key != key2:
                        key = input("Enter a password. This will encrypt all settings and security tokens. Make sure to remember it: ")
                        key2 = input("Enter it again to confirm: ")
                        line_print()
                        if key != key2:
                            print("The entered passwords didn't match. Try again...")
                            line_print()
                        elif key == key2:
                            _key = key_encode(key)
                    auth_method = input("What Authentication Method do you want to use? Enter 1 for Web or 2 for Connected App: ")
                    line_print()
                    config['DEFAULT'] = {'method': '{}'.format(auth_method)}
                with open('auth_method.ini', 'w') as configfile:
                    config.write(configfile)

        if auth_method == "2":
            key = input("Enter your password to decrypt all settings and security tokens: ")
            _key = key_encode(key)
            sfdc_login.setup_ini(flag,_key)
            config_file = "sfdc_auth.ini"
            access_token,server_id,server_domain = sfdc_login.get_token(_key)
            return access_token,server_id,server_domain
        elif auth_method == "1":
            key = input("Enter your password to decrypt all settings and security tokens: ")
            _key = key_encode(key)
            sfdc_login.web_auth(flag,_key)
            config_file = "web_sfdc_auth.ini"
            access_token,server_id,server_domain = sfdc_login.web_get_token(_key)
            if access_token == "INVALID_TOKEN":
                flag = "Y"
                sfdc_login.auth_check(flag)
                flag = "N"
                access_token,server_id,server_domain = sfdc_login.web_get_token(_key)
            return access_token,server_id,server_domain

    def intro():

        print("\r\n")
        line_print_ext()
        time.sleep(0.1)
        prCyan("                            FOSS Analytics Toolkit for Salesfoce REST API ")
        time.sleep(0.3)
        prGreen("                                                 v0.1")
        prCyan("                                       Salesforce API v53.0")
        prPurple("                         https://github.com/pg-dev-git/foss_analytics_toolkit")
        time.sleep(0.5)
        line_print_ext()
        prLightPurple("This is a FOSS tool, not an official Salesforce or Tableau product.\r\n This toolkit hasn't been officially tested or documented by Salesforce or Tableau.\r\n Salesforce support is not available. Use at your own risk.\r\n")
        prYellow("Licensed under: GNU AFFERO GENERAL PUBLIC LICENSE Version 3")
        line_print_ext()
        time.sleep(0.5)
