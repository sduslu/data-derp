import os
from s3fs import S3FileSystem

def download_dataset(s3_uri: str, destination: str):
    """Anonymously downloads a dataset from S3 to a custom destination.
       If any parent directories do not exist, this function will create them.
    """
    filename = destination.strip("/").split("/")[-1]
    folder = destination.replace(filename, "")

    if not os.path.exists(folder):
        os.makedirs(folder) # create destination folder(s) if they don't exist

    s3 = S3FileSystem(anon=True)
    print("Downloading from:", s3_uri)
    print("Downloading to:", destination)
    s3.get(s3_uri.replace("s3://", ""), destination, recursive=True)
    return