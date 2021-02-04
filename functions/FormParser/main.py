from google.cloud import bigquery
from google.cloud import pubsub_v1
import six
import os
import base64
import json
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage

def parse_form(event, context):
    """Parse a form"""

    # Event listing
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    processor_id = os.environ['processor_id']
    location = os.environ['location']
    project_id = os.environ['project_id']
    bq_topic_name= os.environ['bq_topic_name']
    file_path = event['name']
    form_input_bucket = event['bucket']
    publisher = pubsub_v1.PublisherClient()

    # Instantiates a client
    client = documentai.DocumentProcessorServiceClient()

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    image_content = download_blob(form_input_bucket, file_path).download_as_bytes()

    # Read the file into memory
    document = {"content": image_content, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": name, "document": document}

    # Recognizes text entities in the PDF document
    result = client.process_document(request=request)

    document = result.document

    print("Document processing complete.")

    # For a full list of Document object attributes, please reference this page: https://googleapis.dev/python/documentai/latest/_modules/google/cloud/documentai_v1beta3/types/document.html#Document

    document_pages = document.pages

    # Read the text recognition output from the processor
    print("The document contains the following paragraphs:")
    for page in document_pages:
        print('Page number: {}'.format(page.page_number))
        futures = []
        paragraphs = page.paragraphs
        for paragraph in paragraphs:
            paragraph_text = get_text(paragraph.layout, document)

        for form_field in page.form_fields:
            print('********************************')            
            #print('Field Value confidence: {}'.format(form_field.field_value.confidence))            
            text = get_text(form_field.field_name,document)
            text_confidence = form_field.field_name.confidence
            text_value = get_text(form_field.field_value,document)
            if(text_value == ""):
                text_value = "Empty Value (added by code)"
            text_value_confidence = form_field.field_value.confidence
            
            print("Pub/Sub insert page_no: {} form_key: {} form_key_confidence: {} form_value: {} form_value_confidence: {} " .format(page.page_number, text, text_confidence, text_value, text_value_confidence))
            message = {
                'page_no': page.page_number,
                'form_key': text,
                'form_key_confidence': text_confidence,
                'form_value': text_value,
                'form_value_confidence': text_value_confidence,
            }
            
            print('Topic Name: {}'.format(bq_topic_name))
            message_data = json.dumps(message).encode('utf-8')
            topic_path = publisher.topic_path(project_id, bq_topic_name)
            print('Topic Path: {}'.format(topic_path))
            future = publisher.publish(topic_path, data=message_data)
            futures.append(future)
            for future in futures:
                future.result()
            print ("Sent to PubSub")

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    return blob
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
