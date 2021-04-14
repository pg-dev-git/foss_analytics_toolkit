import json
import requests
from terminal_colors import *
import os
import time


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
                prYellow("\r\n"+ "Running first time configuration login configuration" + "\r\n")
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
