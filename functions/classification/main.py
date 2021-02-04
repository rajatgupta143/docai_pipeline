from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
import os
from google.cloud import storage

def startProcess(event, context):
    project_id = os.environ['project_id']
    endpoint_id = os.environ['endpoint_id']
    location = os.environ['location']
    api_endpoint = os.environ['api_endpoint']
    ocr_processor_id = os.environ['ocr_processor_id']
    docai_location=os.environ['docai_location']
    invoice_input_bucket=os.environ['invoice_input_bucket']
    ds5528_input_bucket=os.environ['ds5528_input_bucket']
    file_path = event['name']
    prediction_input_bucket = event['bucket']
    
    print("project_id:{},endpoint_id{}, location:{}, OCR processor id:{}, bucket:{},file_path:{}".format(project_id,endpoint_id, docai_location, ocr_processor_id,prediction_input_bucket,file_path))
    print ("Starting OCR")
    # Getting updated content from the uploaded file....
    content = process_document_sample(project_id,docai_location,ocr_processor_id,prediction_input_bucket,file_path)
    
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    instance_dict = {"content": content}
    instance = json_format.ParseDict(instance_dict, Value())
    instances = [instance]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    endpoint = client.endpoint_path(
        project=project_id, location=location, endpoint=endpoint_id
    )
    response = client.predict(
        endpoint=endpoint, instances=instances, parameters=parameters
    )
    # See gs://google-cloud-aiplatform/schema/predict/prediction/text_classification.yaml for the format of the predictions.
    predictions = response.predictions
    
    predictionDict={}
    for prediction in predictions:
        #print ("Invoice Prediction:{}".format(dict(prediction)["confidences"]))
        predictionDict = dict(prediction)

        confidenceList = prediction["confidences"]
        dispNameList = prediction["displayNames"]
        newDict={}
        i=0
        for confidence in confidenceList:            
            newDict[dispNameList[i]]= confidence
            i=i+1
        newDict=sorted(newDict.items(), key=lambda item: item[1])
        print("Final predicted value:{}".format(newDict[-1][0]))
        predicted_value=newDict[-1][0]

        if(predicted_value=="invoice"):
            rewiteFileToBucket(prediction_input_bucket, file_path, invoice_input_bucket)
            print("Moved the file to:{}".format(invoice_input_bucket))
        elif(predicted_value=="ds5528"):
            rewiteFileToBucket(prediction_input_bucket, file_path, ds5528_input_bucket)
            print("Moved the file to:{}".format(ds5528_input_bucket))

def process_document_sample(
    project_id: str, location: str, processor_id: str, bucket_name: str, file_path: str
):
    from google.cloud import documentai_v1beta3 as documentai

    print("Inside the OCR function, project_id: {}, location: {}, processor_id: {}, bucket_name: {}, file_path: {}".format(project_id,location,processor_id,bucket_name,file_path))
    # Instantiates a client
    client = documentai.DocumentProcessorServiceClient()

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    image_content = download_blob(bucket_name, file_path).download_as_bytes()
    # Read the file into memory
    document = {"content": image_content, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": name, "document": document}

    # Recognizes text entities in the PDF document
    result = client.process_document(request=request)

    document = result.document

    print("Document OCR processing complete: {}/{}".format(bucket_name,file_path))

    # For a full list of Document object attributes, please reference this page: https://googleapis.dev/python/documentai/latest/_modules/google/cloud/documentai_v1beta3/types/document.html#Document

    document_pages = document.pages

    # Read the text recognition output from the processor
    #print("The document contains the following paragraphs:")
    ocrText = ""
    for page in document_pages:
        paragraphs = page.paragraphs
        for paragraph in paragraphs:
            paragraph_text = get_text(paragraph.layout, document)
            #print(f"Paragraph text: {paragraph_text}")
            ocrText=ocrText+paragraph_text
    return ocrText        


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
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return response

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob

def rewiteFileToBucket(source_bucket, source_filename, destination_bucket):
    storage_client = storage.Client()
    sbucket = storage_client.bucket(source_bucket)
    sblob = sbucket.blob(source_filename)
    
    dbucket=storage_client.bucket(destination_bucket)
    dblob=dbucket.blob(source_filename)
    dblob.rewrite(sblob)
