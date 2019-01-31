import os
import tarfile
import click
import shutil
from google.cloud import storage

@click.command('gs-to-tar')
@click.option('--bucket', type=str, help="The Google Storage bucket, without gs:// prefix")
@click.option('--prefix', type=str, 
              help="The filepath to search for yaml files, i.e. geomedian/v2.1.0/L5")
@click.option('--suffix', type=str, default=".yaml", help="The suffix of your metadata files")
@click.option('--outfile', type=str, default="metadata.tar.gz", help="Sets the output file name")
def cli(bucket,
        prefix,
        suffix,
        outfile):
    """ Download Metadata from GS bucket to tarball

    Example:

       \b
       Download files in directory that match `*yaml` and store them as a tar
        > gs-to-tar --bucket data.deadev.com --prefix mangrove_cover

    """

    # Connect to bucket
    client = storage.Client.create_anonymous_client()
    bucket_object = client.bucket(bucket, user_project=None)

    # Get list of files under prefix
    print("Searching {bucket}/{prefix} for files".format(bucket=bucket_object, prefix=prefix))
    blobs = bucket_object.list_blobs(prefix=prefix)

    # Filter on suffix
    files = [blob for blob in blobs if blob.name.endswith(suffix)]
    file_count = str(len(files))
    print("Found {0} metadata files".format(file_count))

    # Download Files to tar with an updating counter
    file_num = 1
    tar = tarfile.open(outfile, "w:gz")
    for yaml in files:

        count = str(file_num)
        filename = "{bucket}/{filepath}.yaml".format(bucket=bucket, filepath=yaml.name)

        #ensure dir exists
        if not os.path.exists(os.path.dirname('./' + filename)):
            os.makedirs(os.path.dirname('./' + filename))
        #download to tar
        yaml.download_to_filename(filename=filename, client=client)
        tar.add(filename)
        os.remove('./' + filename)

        #counter
        print("{count}/{file_count} Downloaded".format(count=count, file_count=file_count))
        file_num += 1

    tar.close()
    # Deletes the directory recursively
    shutil.rmtree("./" + bucket)

    print("Done!")

if __name__ == '__main__':
    cli()