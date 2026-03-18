import hashlib
import os.path
import shutil
import zipfile
from os import listdir
from os.path import isfile, join
import xml.etree.ElementTree as et
from xml.etree.ElementTree import Element
from tinydb import TinyDB, Query
from pyPreservica import *
from xml.dom import minidom
from multiprocessing import Pool
import datetime


MD_FOLDER = "METADATA"
TIF_FOLDER = "MASTER"
PDF_FOLDER = "PDF"

MAX_WORKFLOW_SIZE: int = 4

class MetsFixityCallback:
    def __init__(self, fixity_map: dict, xml_file: str):
        self.fixity_map = fixity_map
        self.xml_file = xml_file

    def __call__(self, filename, full_path):
        if filename == self.xml_file:
            md5 = FileHash(hashlib.md5)
            return "MD5", md5(full_path)
        else:
            return "MD5", self.fixity_map[filename]


DC_METADATA = "http://www.openarchives.org/OAI/2.0/oai_dc/"

def preservation_fixity(root: Element) -> dict:
    file_group_master = root.findall(".//{*}fileSec/{*}fileGrp[@ID='MASTER']")
    assert len(file_group_master) == 1
    master_files = root.findall(".//{*}fileSec/{*}fileGrp[@ID='MASTER']/{*}file")
    number_tifs = len(master_files)
    fixity_map = {}
    for e in master_files:
        fixity_map[
            os.path.basename(e.find(".//{*}FLocat").attrib["{https://www.w3.org/1999/xlink}href"]).split("\\")[-1]] = \
        e.attrib['CHECKSUM'].lower()
    return fixity_map

def dublin_core(root: Element, basename: str):
    title_em_list = root.findall(".//{*}xmlData/{*}mods/{*}titleInfo/{*}title")
    if len(title_em_list) == 1:
        title: str = title_em_list[0].text
        xml_data = root.findall(".//{*}xmlData/{*}dc")
        identifier = root.findall(".//{*}xmlData/{*}dc/{*}identifier")[0].text
        assert identifier == basename
        return title, identifier, minidom.parseString(et.tostring(xml_data[0])).toprettyxml(indent="   ", newl="")
    return ""


def create_sip(folder: Folder, entity: EntityAPI, upload: UploadAPI, process: ProcessAPI, process_id: str, zip_file: str, basename: str, storage_root: str, bucket: str, export_folder: str, num_processed: int):
    
    
    workflow_size = len(list(workflow.workflow_instances(workflow_state="Active", workflow_type="Ingest")))

    while workflow_size > MAX_WORKFLOW_SIZE:
        time.sleep(30)
        workflow_size = len(list(workflow.workflow_instances(workflow_state="Active", workflow_type="Ingest")))
        print(f"Workflow Size: {workflow_size}")

    
    print(f"Found Package {zip_file} Extracting ...")
    extract_path = os.path.join(storage_root, basename)
    with zipfile.ZipFile(join(storage_root, zip_file), 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    if os.path.isdir(extract_path):
        metadata: str = f"{join(join(extract_path, MD_FOLDER), basename)}.mets.xml"
        if os.path.isfile(metadata):
            tree = et.parse(metadata)
            root: Element = tree.getroot()
            title, identifier, dublin_core_xml = dublin_core(root, basename)
            fixity_map = preservation_fixity(root)

            # second check that this asset does not exist before upload
            if len(entity.identifier("Identifier", identifier)) == 0:
                master_files = join(extract_path, TIF_FOLDER)
                preservation_files = [join(master_files, f) for f in listdir(master_files) if
                                      (isfile(join(master_files, f)) and f.endswith(".tif"))]
                preservation_files.sort()

                assert len(preservation_files) == len(fixity_map), "The number of TIF images in the folder does not match the METS file"

                pdf_files = join(extract_path, PDF_FOLDER)
                access_files = [join(pdf_files, f) for f in listdir(pdf_files) if
                                (isfile(join(pdf_files, f)) and f.endswith(".pdf"))]
                access_files.sort()

                print(f"{zip_file} Extraction complete. Creating Package ...")
                package = generic_asset_package(preservation_files_dict={"TIF Images": preservation_files, "METS Metadata": [metadata]},
                                                access_files_dict={"PDF Document": access_files},
                                                Title=title, Asset_Metadata={DC_METADATA: dublin_core_xml},
                                                Identifiers={"Identifier": identifier}, parent_folder=folder,
                                                export_folder=export_folder, Preservation_files_fixity_callback=MetsFixityCallback(fixity_map, f"{basename}.mets.xml"))
                
                upload.upload_zip_package_to_S3(path_to_zip_package=package, bucket_name=bucket, folder=folder,
                                                callback=UploadProgressConsoleCallback(package),
                                                delete_after_upload=True)


                # re-active the ingest workflow
                if num_processed % 7 == 0:
                    process.deactivate_process(process_id)
                    process.reactivate_process(process_id)

                # delete the tmp folder
                shutil.rmtree(extract_path)



def main():

    # Create the pyPreservica SDK clients
    upload: UploadAPI = UploadAPI()
    process: ProcessAPI = ProcessAPI()
    entity: EntityAPI = EntityAPI()
    workflow: WorkflowAPI = WorkflowAPI()

    # Load the configuration settings
    storage_root: str = entity.config['data']['storage_root']
    collection_id: str = entity.config['data']['collection_id']
    bucket: str = entity.config['data']['bucket']
    pool_size: int = int(entity.config['data']['pool_size'])
    export_folder: str = entity.config['data']['export_folder']
    workflow_name: str = entity.config['data']['workflow_name']

    db:  TinyDB = TinyDB('ingested-content.json')
    query: Query = Query()

    # Check we have a valid ingest workflow
    process_id = None
    for p in process.ingest_process(ingest_types=["PACKAGE"]):
        if p.name == workflow_name:
            process_id = p.process_id
            assert p.trigger_type == "WATCHER", f"The ingest workflow {workflow_name} should be set to auto-start"
            break

    if process_id is None:
        print(f"Could not find a valid ingest workflow")
        return

    # activate the ingest workflow
    process.reactivate_process(process_id)

    # Create a thread pool for running the ingests
    pool = Pool(processes=pool_size)

    folder = entity.folder(collection_id)

    print(f"Looking for zip packages inside folder {storage_root}")

    num_processed: int = 0
    for zip_file in [f for f in listdir(storage_root) if (isfile(join(storage_root, f)) and f.endswith(".zip"))]:
        basename: str = os.path.basename(zip_file).replace(".zip", "")

        # Check the TinyDB database first for ingest content as its quicker
        results = db.search(query.identifier == basename)
        if len(results) > 0:
            for result in results:
                print(f"Package {result['zip_file']} ingested with Preservica ID {result['ref']} Skipping ...")
        else:
            assets = entity.identifier("Identifier", basename)
            if len(assets) == 0:
                print(f"Adding Package {zip_file} to the Queue ...")
                num_processed = num_processed + 1
                pool.apply_async(func=create_sip, args=(folder, entity, upload, process, process_id, zip_file, basename, storage_root, bucket, export_folder, num_processed))
            else:
                print(f"Package {zip_file} already ingested. Skipping ...")
                # Add information to the database
                for a in assets:
                    db.insert({'identifier': basename, 'zip_file': zip_file, 'ref': a.reference, 'title': a.title, 'date-time': str(datetime.datetime.now())})

    # Close the pool and wait for all the ingests to finish
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
