import json, time, re, math
import pandas as pd

def csv_upload_json_meta(dataset_name_,dataset_name):

    try:
        if os.path.exists('{}_CSV_upload_metadata.json'.format(dataset_name)):
            os.remove('{}_CSV_upload_metadata.json'.format(dataset_name))
    except:
        pass

    #Define regex for date match yyyy/MM/dd only
    csv_date_match = '((19|20)\d{2})[-/.](0[1-9]|1[012])[-/.](0[1-9]|[12][0-9]|3[01])'

    #Initialize json elements
    json_metadata = {}
    json_fileFormat = {}
    json_objects = []
    json_fields = []

    #Load CSV and parse elements
    load_csv = pd.read_csv("{}.csv".format(dataset_name), low_memory=False, nrows=1)
    csv_headers_list = list(load_csv)
    csv_columns_list = load_csv.values.tolist()

    #Create JSON elements for each field in the CSV file
    c1 = 0
    c2 = 1
    for x in csv_headers_list:
        header = csv_headers_list[c1]
        col_value = csv_columns_list[0][c1]
        c1 += 1
        #print(header)
        #print(col_value)
        #time.sleep(0.1)
        try:

            #Check if the field is a date in yyyy/MM/dd format only
            if type(col_value) != int and type(col_value) != float and type(col_value) != bool and (re.match("{}".format(csv_date_match),col_value)) is not None:
                data_type = "Date"

                if col_value.find("-") >= 0:
                    c = "-"
                elif col_value.find("/") >= 0:
                    c = "/"

                time_flag = 0

                if col_value.find("T") >= 0:
                    time_flag = 1
                else:
                    time_flag = 0

                idx = []
                for pos,char in enumerate(col_value):
                    if(char == c):
                        idx.append(pos)

                if c == "-" and idx[0] == 4 and idx[1] == 7 and time_flag == 0:
                    date_format = "yyyy-MM-dd"
                elif c == "-" and idx[0] == 3 and idx[1] == 6 and time_flag == 0:
                    date_format = "MM-dd-yyyy"
                elif c == "-" and idx[0] == 4 and idx[1] == 7 and time_flag == 1:
                    date_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                elif c == "-" and idx[0] == 3 and idx[1] == 6 and time_flag == 1:
                    date_format = "MM-dd-yyyy'T'HH:mm:ss.SSS'Z'"

                if c == "/" and idx[0] == 4 and idx[1] == 7 and time_flag == 0:
                    date_format = "yyyy/MM/dd"
                elif c == "/" and idx[0] == 3 and idx[1] == 6 and time_flag == 0:
                    date_format = "MM/dd/yyyy"
                elif c == "/" and idx[0] == 4 and idx[1] == 7 and time_flag == 1:
                    date_format = "yyyy/MM/dd'T'HH:mm:ss.SSS'Z'"
                elif c == "/" and idx[0] == 3 and idx[1] == 6 and time_flag == 1:
                    date_format = "MM/dd/yyyy'T'HH:mm:ss.SSS'Z'"

                json_fields.append({"description": "",
                                    "fullyQualifiedName": "{}".format(header),
                                    "label": "{}".format(header),
                                    "name": "{}".format(header),
                                    "type": "{}".format(data_type),
                                    "format": "{}".format(date_format)})

            #Check if the cell is empty and set it as text
            try:
                if col_value in (None,"") or (math.isnan(col_value)) == True:
                    data_type = "Text"
                    json_fields.append({"description": "",
                                    "fullyQualifiedName": "{}".format(header),
                                    "label": "{}".format(header),
                                    "name": "{}".format(header),
                                    "type": "{}".format(data_type)})
            except:
                pass

            #Check if the field is a number
            if (type(col_value) == int or type(col_value) == float) and (math.isnan(col_value)) != True:
                data_type = "Numeric"
                json_fields.append({"description": "",
                                "fullyQualifiedName": "{}".format(header),
                                "label": "{}".format(header),
                                "name": "{}".format(header),
                                "defaultValue": "0",
                                "type": "{}".format(data_type),
                                "precision": 10,
                                "scale": 2})

            #Check if the field is a string
            if (type(col_value) == str and re.match("{}".format(csv_date_match),col_value) is None) or (type(col_value) == bool):
                data_type = "Text"
                json_fields.append({"description": "",
                                "fullyQualifiedName": "{}".format(header),
                                "label": "{}".format(header),
                                "name": "{}".format(header),
                                "type": "{}".format(data_type)})
        except ValueError:
            pass


    #Set up Json Metadata Format
    json_fileFormat["charsetName"] = "UTF-8"
    json_fileFormat["fieldsEnclosedBy"] = "\""
    json_fileFormat["fieldsDelimitedBy"] = ","
    json_fileFormat["numberOfLinesToIgnore"] = 1

    #Append Fields to Objects
    json_objects.append({"connector" : "TCRM_Toolkit",
                         "description" : "",
                         "fullyQualifiedName": "{}".format(dataset_name_),
                         "label": "{}".format(dataset_name_),
                         "name": "{}".format(dataset_name_),
                         "fields": json_fields})

    #Add all elements to final json
    json_metadata['fileFormat'] = json_fileFormat
    json_metadata['objects'] = json_objects

    #Save metadata as a JSON file
    with open('{}_CSV_upload_metadata.json'.format(dataset_name), 'w') as outfile:
        json.dump(json_metadata, outfile)
