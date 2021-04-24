import json
import requests
from terminal_colors import *
import os
import time
from art import *


class sfdc_login():

    def get_token():
        import configparser

        config = configparser.ConfigParser()
        config.read("sfdc_auth.ini")
        client_id = config.get("DEFAULT", "client_id")
        client_secret = config.get("DEFAULT", "client_secret")
        username = config.get("DEFAULT", "username")
        password = config.get("DEFAULT", "password")

        data = {
          'grant_type': 'password',
          'client_id': '{}'.format(client_id),
          'client_secret': '{}'.format(client_secret),
          'username': '{}'.format(username),
          'password': '{}'.format(password)
        }
        #print(data)
        #resp = requests.post('https://na91.salesforce.com/services/oauth2/token', data=data)
        resp = requests.post('https://login.salesforce.com/services/oauth2/token', data=data)

        #print(resp.json())

        #Print PrettyJSON in Terminal
        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)

        #prGreen(formatted_response_str)
        #global access_token
        access_token = formatted_response.get("access_token")
        #print(access_token)
        return access_token

    def setup_ini(flag):
        import configparser
        import getpass

        client_id = "Enter your client id"
        client_secret = "Enter your client secret"
        username = "Enter your username"
        password = "Enter your password + token"
        server_id = "Enter you server id \"na100\""

        if flag == "Y":
            os.remove("sfdc_auth.ini")

        while client_id == "Enter your client id" and client_secret == "Enter your client secret" and username == "Enter your username" and password == "Enter your password + token" and server_id == "Enter you server id \"na100\"":

            if os.path.exists("sfdc_auth.ini") == False:
                prYellow("\r\n"+ "Running first time configuration of login credentials..." + "\r\n")
                time.sleep(3)
                config = configparser.ConfigParser()
                config['DEFAULT'] = {'client_id': '{}'.format(client_id),
                                     'client_secret': '{}'.format(client_secret),
                                     'username': '{}'.format(username),
                                     'password': '{}'.format(password),
                                     'server_id': '{}'.format(server_id)}
                with open('sfdc_auth.ini', 'w') as configfile:
                    config.write(configfile)

            elif os.path.exists("sfdc_auth.ini"):
                config = configparser.ConfigParser()
                config.read("sfdc_auth.ini")
                client_id = config.get("DEFAULT", "client_id")
                client_secret = config.get("DEFAULT", "client_secret")
                username = config.get("DEFAULT", "username")
                password = config.get("DEFAULT", "password")
                if client_id == "Enter your client id" and client_secret == "Enter your client secret" and username == "Enter your username" and password == "Enter your password + token" and server_id == "Enter you server id \"na100\"":
                    client_id = getpass.getpass("\r\n"+ "Enter your client id: ")
                    client_secret = getpass.getpass("\r\n"+ "Enter your client secret: ")
                    username = getpass.getpass("\r\n"+ "Enter your username: ")
                    password = getpass.getpass("\r\n"+ "Enter your password: ")
                    server_id = input("\r\n"+ "Enter you server id \"na100\": ")
                    config['DEFAULT'] = {'client_id': '{}'.format(client_id),
                                         'client_secret': '{}'.format(client_secret),
                                         'username': '{}'.format(username),
                                         'password': '{}'.format(password),
                                         'server_id': '{}'.format(server_id)}
                    with open('sfdc_auth.ini', 'w') as configfile:
                        config.write(configfile)

    def web_auth(flag):
        import subprocess
        import configparser

        username = "Enter your username"
        server_id = "Enter you server id \"na100\""
        access_token = "Value_Replace"

        if flag == "Y":
            os.remove("web_sfdc_auth.ini")

        while username == "Enter your username" and server_id == "Enter you server id \"na100\"" and access_token == "Value_Replace":

            if os.path.exists("web_sfdc_auth.ini") == False:
                prYellow("\r\n"+ "Running first time configuration of login credentials..." + "\r\n")
                time.sleep(3)
                config = configparser.ConfigParser()
                config['DEFAULT'] = {'username': '{}'.format(username),
                                     'server_id': '{}'.format(server_id),
                                     'access_token': '{}'.format(access_token)}
                with open('web_sfdc_auth.ini', 'w') as configfile:
                    config.write(configfile)
            elif os.path.exists("web_sfdc_auth.ini"):
                config = configparser.ConfigParser()
                config.read("web_sfdc_auth.ini")
                username = config.get("DEFAULT", "username")
                server_id = config.get("DEFAULT", "server_id")
                access_token = config.get("DEFAULT", "access_token")
                if username == "Enter your username" and server_id == "Enter you server id \"na100\"" and access_token == "Value_Replace":
                    username = input("\r\n"+ "Enter your username: ")
                    server_id = input("\r\n"+ "Enter you server id \"na100\": ")
                    config['DEFAULT'] = {'username': '{}'.format(username),
                                         'server_id': '{}'.format(server_id),
                                         'access_token': '{}'.format(access_token)}
                    with open('web_sfdc_auth.ini', 'w') as configfile:
                        config.write(configfile)

    def web_get_token():
        import configparser
        import subprocess

        config = configparser.ConfigParser()
        config.read("web_sfdc_auth.ini")
        username = config.get("DEFAULT", "username")
        server_id = config.get("DEFAULT", "server_id")
        access_token = config.get("DEFAULT", "access_token")

        if access_token == "Value_Replace":

            sfdc_login_command = subprocess.run(["sfdx", "force:auth:web:login", "-r", "https://{}.salesforce.com".format(server_id)], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)

            log_credentials = subprocess.run(["sfdx", "force:org:display", "-u", "{}".format(username), "--json"], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)

            access_token = json.loads(log_credentials.stdout)

            access_token = access_token.get("result").get("accessToken")

            config['DEFAULT'] = {'username': '{}'.format(username),
                                 'server_id': '{}'.format(server_id),
                                 'access_token': '{}'.format(access_token)}

            with open('web_sfdc_auth.ini', 'w') as configfile:
                config.write(configfile)

            return access_token

        elif access_token != "Value_Replace":

            headers = {
                'Authorization': "Bearer {}".format(access_token)
                }
            resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave'.format(server_id), headers=headers)
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
                    return access_token
            except:
                return access_token

    def auth_check(flag):

        import configparser

        auth_method = "999"

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
                    auth_method = input("What Authentication Method do you want to use? Enter 1 for Web or 2 for Connected App: ")
                    config['DEFAULT'] = {'method': '{}'.format(auth_method)}
                with open('auth_method.ini', 'w') as configfile:
                    config.write(configfile)

        if auth_method == "2":
            sfdc_login.setup_ini(flag)
            config_file = "sfdc_auth.ini"
            access_token = sfdc_login.get_token()
            return config_file,access_token
        elif auth_method == "1":
            sfdc_login.web_auth(flag)
            config_file = "web_sfdc_auth.ini"
            access_token = sfdc_login.web_get_token()
            if access_token == "INVALID_TOKEN":
                flag = "Y"
                sfdc_login.auth_check(flag)
                flag = "N"
                access_token = sfdc_login.web_get_token()
            return config_file,access_token

    def intro():

        print("\r\n")
        time.sleep(0.1)
        tprint("TCRM",font="block")
        time.sleep(0.5)
        tprint("     Tool Kit     ")
        time.sleep(0.5)
        prGreen("                                                                       v0.1-beta3" + "\r\n")
        prCyan("                                                             Salesforce API v51.0")
        print("\r\n")
        time.sleep(0.5)
