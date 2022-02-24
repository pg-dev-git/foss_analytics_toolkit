# TCRM Toolkit

### First things first: This is a beta product. It may contain bugs and unfinished features so use it at your own risk. It's provided as is and without any type of warranties.

This toolkit has the purpose of expand the usabilty of TCRM. There are many tasks that are difficult to do using the UI like uploading CSVs or backing up data. The goal is to make those tasks easy to complete.

#### ----------------------------------------------------------------------------------------------------------------
#### Dependencies:
You need to have Salesforce CLI installed. Get it from here: https://developer.salesforce.com/tools/sfdxcli

##### Notes for Win10: 
You need to install *Windows Terminal* from the Microsoft App Store. There are colors and functions in the app that won't work in command prompt. You can get it here: https://www.microsoft.com/en-us/p/windows-terminal/9n0dx20hk701

Also, make sure you have the Visual C++ Redist installed. Get it from here: https://aka.ms/vs/16/release/vc_redist.x64.exe
#### ----------------------------------------------------------------------------------------------------------------

After installing Windows Terminal, reboot and now you should have an option to open the terminal when you right click inside a directory.
Navigate to the folder you extracted the tool, right click and launch Windows Terminal.
Then just launch TCRM_toolkit.exe from it.

### At this time, the only date format supported when uploading CSV files is: yyyy/mm/dd. If another format is used, the field will be formatted as text.

## Login instructions

There are two ways how to authenticate. Web Login and via a Connected app. The Web Login is the easier and recommended way.

### Instructions for Web Login:

When you select this option on the console, enter your instance username and the server id. Your browser will open up the Salesforce login screen. Enter your credentials and you should be good to go. You can close the browser afterwards. *Make sure your user has a TCRM license and access to the Wave API*

### Instructions for Connected App: https://github.com/pg-dev-git/tcrm_toolkit/blob/main/conn_app.md

## Data Extraction Performance

When this tool is not targeted to execute massive "ETL" jobs, it can perform decent downloads. If you want to download big datasets, you will need a lot of ram.
The following numbers were obtained on a limited virtual machine with 6 CPU Cores and 8GB of ram.

![alt text](https://github.com/pg-dev-git/tcrm_toolkit_source/blob/main/readme_images/performance_virtual.png)
