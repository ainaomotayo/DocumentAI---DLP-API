# Extract pdf with document AI and mask the PII data with DLP API
from google.api_core.client_options import ClientOptions
from google.cloud import documentai
import google.cloud.dlp
import datetime
import os
import io
from google.cloud import storage
from google.cloud.dlp import CharsToIgnore

def extract_masked(project_id: str, location: str, processor_id: str, stream: str, mime_type: str, field_mask: str = None, masking_character=None, number_to_mask=0):

    g_location = 'us'
    opts = ClientOptions(api_endpoint=f"{g_location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the processor, e.g.:
    # projects/{project_id}/locations/{location}/processors/{processor_id}
    name = client.processor_path(project_id, location, processor_id)

    # Read the file into memory
    with open(stream, "rb") as image:
        image_content = image.read()

    # Load Binary Data into Document AI RawDocument Object
    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)

    # Configure the process request
    request = documentai.ProcessRequest(
        name=name, raw_document=raw_document, field_mask=field_mask
    )

    result = client.process_document(request=request)
    document = result.document

# [START dlp_deidentify_masking]
    # Instantiate a client
    dlp = google.cloud.dlp_v2.DlpServiceClient()

    # Convert the project id into a full resource id.
    location_id = 'us-central1'
    parent = f"projects/{project_id}/locations/{location_id}"

    # Construct inspect configuration dictionary
    info_types = ['ALL_BASIC']
    inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}

    # Construct deidentify configuration dictionary
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "character_mask_config": {
                            "masking_character": masking_character,
                            "number_to_mask": number_to_mask,
                        }
                    }
                }
            ]
        }
    }

    # Construct item
    item = {"value": document.text}

    # Call the API
    response = dlp.deidentify_content(
        request={
            "parent": parent,
            "deidentify_config": deidentify_config,
            "inspect_config": inspect_config,
            "item": item,
        }
    )

    # Print out the results.
    # print(response.item.value)

    # GCP_PROJECT = os.getenv('GCP_PROJECT', 'traccar-156417')
    BUCKET_DST = os.getenv('BUCKET_DST', 'apps-2023')
#     if not GCP_PROJECT or not BUCKET_DST:
#         raise Exception('Missing "project id" or "bucket dest name" in env.')
        
    # put deidentified extract data as txt file.
    blob_name = 'tpdf1.txt'
    # file_src = stream['name']
    # file_dst = '{}_{}.txt'.format(os.path.splitext(file_src)[0], datetime.now().strftime('%Y%m%d%H%M%S'))
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_DST)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(response.item.value)
    f.close()
extract_masked(project_id = 'traccar-156417', location = 'us', processor_id = 'c3c358e048fb751a', stream = 'Winnie_the_Pooh_3_Pages.pdf', mime_type = 'application/pdf', number_to_mask=0)
