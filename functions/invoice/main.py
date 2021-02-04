from google.cloud import documentai_v1beta3 as documentai, bigquery
import simplejson as json
import proto
from google.cloud import storage
import os

def startInvoiceParser(event, context):
    project_id = os.environ['project_id']
    processor_id = os.environ['processor_id']
    location = os.environ['location']
    table_id = os.environ['BQ_Table_Id']
    file_path = event['name']
    invoice_input_bucket = event['bucket']

    print("project_id:{},processor_id:{},file_path:{}, invoice bucket:{}".format(project_id,processor_id,file_path,invoice_input_bucket))
    
    # Instantiates a client
    client = documentai.DocumentProcessorServiceClient()

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    image_content = download_blob(invoice_input_bucket, file_path).download_as_bytes()

    # Read the file into memory
    document = {"content": image_content, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": name, "document": document}

    # Recognizes text entities in the PDF document
    result = client.process_document(request=request)
    
    document = result.document
    
    entityDict={}
    lineItem_text=""

    for entity in document.entities:
        entity_type = entity.type_
        
        if(entity.normalized_value.text!=""):
            entity_text = entity.normalized_value.text
        else:
            entity_text = entity.mention_text
        
        # Placeholder code below to test whether the amount fields have strings with commas coming in. Converting them to floats for now.        
        if("amount" in entity_type and entity.normalized_value.text ==''):
            entity_text = float(entity.mention_text.replace(',',''))
            #print("Entity Type:{},entity_text:{}".format(entity_type, entity_text))

        if(entity_type=="line_item"):
            lineItem_text =  lineItem_text+'{'
            currentLIKeys=""
            for prop in entity.properties:                
                pName=prop.type_[prop.type_.index("/")+1:]
                pName=getLineItemKeyName(currentLIKeys, pName)
                if("skip" not in pName):
                    if("amount" in pName and prop.normalized_value.text ==''):
                        prop.mention_text = float(prop.mention_text.replace(',',''))
                    elif(prop.normalized_value.text!=""):
                        prop.mention_text =prop.normalized_value.text
                    lineItem_text = lineItem_text+ "\""+pName +"\""+":"+ "\""+prop.mention_text+ "\""+","   
                    currentLIKeys=currentLIKeys+pName
            lineItem_text = lineItem_text[0:lineItem_text.rindex(",")]
            lineItem_text = lineItem_text+'},'
            #print("lineItem_text text before:{}".format(lineItem_text))
        if(entity_type!="line_item"):
            entityDict[entity_type]=entity_text    
    
    if(lineItem_text!=""):
        lineItem_text = lineItem_text[0:lineItem_text.rindex(",")]
        lineItem_text = "["+lineItem_text+"]"    
        #Take out any special characters
        lineItem_text = lineItem_text.replace('\n', '')    
        lineItem_t = json.loads(lineItem_text)
    
        #print("Final Line Item:{}".format(lineItem_t))
        entityDict["line_item"]=lineItem_t    
    
    document_pages = document.pages

    #Calling the WiteToBQ Method
    writeToBQ(entityDict, table_id)  

# Write to BQ Method
def writeToBQ(documentEntities: dict, table_id):
    print("Inserting into BQ ************** ")
    #Insert into BQ    
    client = bigquery.Client()        
    table = client.get_table(table_id)

    print ('Adding the row')
    rows_to_insert= [documentEntities]

    print (' ********** NEW Row Column: ',rows_to_insert)
    errors = client.insert_rows_json(table, rows_to_insert) 
    if errors == []:
        print("New rows have been added.") 
    else:
        print ('Encountered errors: ',errors)


# Extract shards from the text field
def get_text(doc_element: dict, document: dict):
    """
    Document AI identifies form fields by their offsets
    in document text. This function converts offsets
    to text snippets.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in doc_element.text_anchor.text_segments:
        #print("Segments: ---> ",doc_element.text_anchor.text_segments)
        #print("Segment start index: ---> ",segment.start_index)
        start_index = (
            int(segment.start_index)            
            if segment in doc_element.text_anchor.text_segments else 0
            #if segment.start_index in doc_element.text_anchor.text_segments else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]   
        #print ("Start Index:{}, End Index:{}".format(start_index, end_index))
    #print ("returning text seg resp: {}".format(response))
    return response

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    return blob
    
def getLineItemKeyName(lineItem, key):
    if(key+"3" in lineItem):
        return "skip"
    elif(key+"2" in lineItem):
        return key+"3"
    elif(key in lineItem):
        return key+"2"
    else:
        return key
# [END documentai_process_document]
