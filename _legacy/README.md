### First things first: 

### This is a FOSS tool, not an official Salesforce or Tableau product. This toolkit hasn't been officially tested or documented by Salesforce or Tableau. Salesforce support is not available. Use at your own risk. It's provided as is and without any type of warranties.

This toolkit has the purpose of expand the usability of TCRM. There are many tasks that are difficult to do using the UI like uploading CSVs or backing up data. The goal is to make those tasks easy to complete.

#### ----------------------------------------------------------------------------------------------------------------

### Dependencies:
You need to have Salesforce CLI installed. Get it from here: https://developer.salesforce.com/tools/sfdxcli

#### Notes for Win10: 
You need to install *Windows Terminal* from the Microsoft App Store. There are colors and functions in the app that won't work in command prompt. You can get it here: https://www.microsoft.com/en-us/p/windows-terminal/9n0dx20hk701

After installing Windows Terminal, reboot and now you should have an option to open the terminal when you right click inside a directory.
Navigate to the folder you extracted the tool, right click and launch Windows Terminal.
Then just launch TCRM_toolkit.exe from it.

Also, make sure you have the Visual C++ Redist installed. Get it from here: https://aka.ms/vs/16/release/vc_redist.x64.exe

### Python:
The recommended version of Python is 3.9 but you can use 3.8 too. Python 3.10 won't work at the moment.

You can run "python3.9 -m pip install -r requirements.txt" to install the required dependencies for the tool to run properly.

#### ----------------------------------------------------------------------------------------------------------------

### Compatibility:
This tool is able to run on Windows/Linux/MacOS without any issues. If you find a bug, please report it.

#### ----------------------------------------------------------------------------------------------------------------

### At this time, the only date format supported when uploading CSV files is: yyyy/mm/dd. If another format is used, the field will be formatted as text.

#### ----------------------------------------------------------------------------------------------------------------

## Login instructions

There are two ways how to authenticate. Web Login and via a Connected app. The Web Login is the easier and recommended way.

You will need the server id from the Company Information section of your instance and also the domain name.

### Instructions for Web Login:

When you select this option on the console, enter your instance username and the server id. Your browser will open up the Salesforce login screen. Enter your credentials and you should be good to go. You can close the browser afterwards. *Make sure your user has a TCRM license and access to the Wave API*

### Instructions for Connected App: https://github.com/pg-dev-git/foss_analytics_toolkit/blob/master/conn-app.md

#### ----------------------------------------------------------------------------------------------------------------

## Security

All config files will be encrypted with a password that you set up on the first run. If you forget the password, just delete the config files in the data folder and start from scratch.

#### ----------------------------------------------------------------------------------------------------------------

## Contact

You can reach out via LinkedIn: https://www.linkedin.com/in/pedro-gagliardi-a9b95638/
Or submit a PR here on GitHub

#### ----------------------------------------------------------------------------------------------------------------

## Data Extraction and Upload Performance

When this tool is not targeted to execute massive "ETL" jobs, it can perform decent extractions/uploads. 
If you want to download big datasets, you will need a lot of RAM.
The following numbers were obtained on Windows Desktop with 16 cores and 32gb of RAM and a Ubuntu Desktop with 8 cores and 16gb of RAM.
The tool will automatically try to use disk space in case you run out of RAM but it could also help if you manually increase the size of your SWAP.

![alt text](https://i.ibb.co/CMptHth/perf-table.jpg)

![alt text](https://i.ibb.co/vQnwHNg/16.jpg)

![alt text](https://i.ibb.co/kGtNx3g/32.jpg)
