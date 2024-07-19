import traceback
import time
import json
import getopt
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
sys.path.append("../client_code")
import eazyml as ez


AUTH_FILE = "authentication.json"
UPLOAD_FILE = "upload_document.json"
EXTRACT_FILE = "extract_information.json"


def eazyml_upload(token, document_path, index_name, overwrite="no",
                  prefix_name=""):
    """
    Upload document on EazyML

    Input:
        token: Authentication token
        document_path: Absolute path of the document.
        index_name: New or pre-existing index_name to save the document.
        overwrite: A (yes/no) flag that indicates whether to overwrite the
                   embeddings stored for the specified document.

    Return:
        Returns a boolean to inform whether vector embeddings are stored.
    """
    options = {
               "overwrite": overwrite
              }

    if not os.path.exists(document_path):
        print("Document doesn't exist - %s" % document_path)
        return None
    dump_file = prefix_name + "_" + UPLOAD_FILE
    if os.path.exists(dump_file):
        resp = json.load(open(dump_file))
        index = resp["indexed"]
        print("Returning from cache", dump_file)
        return index
    time_start = datetime.now()
    print("Indexing the document ...")
    resp = ez.ez_upload_document(token, document_path, index_name, options)
    print (resp)
    if resp["success"] is True:
        print("The document is indexed successfully to EazyML from path: %s" %
              (document_path))
        index = resp["indexed"]
        json_obj = json.dumps(resp, indent=4)
        dump_file = prefix_name + "_" + UPLOAD_FILE
        with open(dump_file, "w") as fw:
            fw.write(json_obj)
        print("The response is stored in %s" % (dump_file))
    else:
        print("Upload document error: %s" % (resp["message"]))
        index = False
    print("Indexing document time: " +
          "%.2f secs" % (datetime.now() - time_start).total_seconds())
    print("A Boolean flag indicating whether document is stored " +
          "(indexed) is: %s" % (resp["indexed"]))
    print("Likely next steps:")
    print("    python eazyml_upload_extract_information.py " +
          "--query %s --index_name %s --extract_information" % (
          query, index_name))
    print ("Please provide an existing index name that you mentioned " +
           "while uploading a document.")
    return index


def eazyml_extract_information(token, api_name, query, index_name, 
                               prefix_name=""):
    """
    To extract information from the document on the basis of query.

    Input:
        token: Authentication token
        api_name: Name of the API
        query: Query to be queried in the indexed embeddings.
        index_name: index name of the saved document in which the
                    query has to be performed.

    Return:
        Return the answer of the query.
    """
    if api_name == "extract_information":
        options = {}
        dump_file = prefix_name + "_" + EXTRACT_FILE
        if os.path.exists(dump_file):
            resp = json.load(open(dump_file))
            if "answer" in resp:
                answer = resp["answer"]
            print("Returning from cache", dump_file)
            return answer, resp
        print("Extracting Information ")
        # Extracting information
        time_start = datetime.now()
        resp = ez.ez_extract_information(token, query, index_name, options)
        if resp["success"] is True:
            print("Information extracted successfully")
            json_obj = json.dumps(resp, indent=4)
            dump_file = prefix_name + "_" + EXTRACT_FILE
            with open(dump_file, "w") as fw:
                fw.write(json_obj)
            print("The response is stored in %s" % (dump_file))
        else:
            print("Information Extraction error: %s" % (resp["message"]))
            return None, None

        print("Information Extracting time: " +
            "%.2f secs" % (datetime.now() - time_start).total_seconds())
        answer = resp["answer"]
        print("The answer retrieved for provided query from the "
              "document is: %s" % (answer))
        # print("Likely next steps:")
        # print("    python eazyml_upload_extract_information.py " +
        #       "--index_name %s --get_index_details" % (index_name))
    return answer, resp


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def eazyml_auth(username, api_key, password=None, store_info=False):
    """
    Authenticate and store auth info in a file for future use

    Input:
        username: Email Id or username provided
        api_key: Api Key downloaded from UI
        store_info: Flag to store info for future use

    Return:
        Return authentication token used for sucessive calls to EazyML
    """
    if password is not None:
        resp = ez.ez_auth(username, password=password)
        content = {"username": username,
                   "api_key": api_key,
                   "password": password
                  }

    else:
        resp = ez.ez_auth(username, api_key=api_key)
        content = {"username": username,
                   "api_key": api_key,
                   "password": password
                  }

    if resp["success"] is True:
        print("Authentication successful")
        if store_info:
            json_obj = json.dumps(content, indent=4)
            with open(AUTH_FILE, "w") as fw:
                fw.write(json_obj)
            print("Authentication information is stored in %s" % (AUTH_FILE))
    else:
        print("Authentication error: %s" % (resp["message"]))
        return None
    return resp["token"]


def flow(username, api_key, config_file=None, prefix_name="",
         document_path=None, index_name=None, overwrite="no",
         api_name=None, query=None):
    """
    Run the EazyML operations based on the input

    """
    # Get Authentication token
    token = None
    check_complete_flow = 0
    if username and api_key:
        token = eazyml_auth(username, api_key, store_info=True)
    else:
        if os.path.exists(AUTH_FILE):
            auth_info = json.load(open(AUTH_FILE, "r"))
            token = eazyml_auth(auth_info["username"],
                                auth_info["api_key"],
                                auth_info["password"]
                               )
        else:
            print("Please authenticate to proceed")
            return

    # Set config file
    if config_file:
        print("Uploading the configuration file ...")
        resp = ez.ez_config(token, config_file)
        if resp["success"] is True:
            print("Configuration file is uploaded successfully")
        else:
            print("Configuration file upload error: %s" % (resp["message"]))
            return

    # upload document
    if document_path:
        if not index_name:
            print("Please provide the index name")
            return
        index = eazyml_upload(token, document_path, index_name,
                              overwrite, prefix_name)
        if not index:
            return
        check_complete_flow += 1

    # Extract Information
    if api_name:
        if not query:
            print("Please provide query to be asked in double quotes")
            return
        if not index_name:
            print("Please provide index_name that was mentioned while" 
                    " uploading document")
        success = eazyml_extract_information(token, api_name, query, 
                                            index_name, 
                                            prefix_name=prefix_name)
        if not success:
            return


if __name__ == "__main__":
    args_list = sys.argv[1:]
    # Options
    options = "h:u:p:g:" + \
              "x:d:o:" + \
              "e:i:q:"
    long_options = ["help", "username=", "api_key=", "config_file=",
                    "prefix_name=", "document_path=", "overwrite=",
                    "extract_information", "index_name=", "query="]
    username = api_key = config_file = None
    overwrite = "no"
    document_path = index_name = query = api_name = None
    prefix_name = "EazyML"
    try:
        # Parsing argument
        arguments, values = getopt.getopt(args_list, options, long_options)
        # checking each argument
        for curr_arg, curr_val in arguments:
            print(curr_arg, curr_val)
            if curr_arg in ("-h", "--help"):
                print("".join(open("help.txt", "r").readlines()))
                exit()
            elif curr_arg in ("-u", "--username") and (
                not curr_val.startswith('--')):
                username = curr_val
            elif curr_arg in ("-p", "--api_key") and (
                not curr_val.startswith('--')):
                api_key = curr_val
            elif curr_arg in ("-g", "--config_file"):
                config_file = curr_val
            elif curr_arg in ("-x", "--prefix_name") and (
                not curr_val.startswith('--')):
                prefix_name = curr_val
            elif curr_arg in ("-d", "--document_path") and (
                not curr_val.startswith('--')):
                document_path = curr_val
            elif curr_arg in ("-o", "--overwrite") and (
                not curr_val.startswith('--')):
                overwrite = curr_val
            elif curr_arg in ("-e", "--extract_information"):
                api_name = "extract_information"
                print("API name: ",api_name)
            elif curr_arg in ("-i", "--index_name") and (
                not curr_val.startswith('--')):
                index_name = curr_val
            elif curr_arg in ("-q", "--query") and (
                not curr_val.startswith('--')):
                query = curr_val
        os.system("rm -f " + prefix_name + "*.json")
        flow(username, api_key, config_file, prefix_name,
             document_path, index_name, overwrite,
             api_name, query)
    except getopt.error as err:
        # output error, and return with an error code
        print (str(err))
