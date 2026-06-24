Put your AIStor Free license file in this folder, named exactly:

    minio.license

Request a free-tier license from MinIO, save it here, and the compose file
will mount it into the minio container.

The data/ subfolder is MinIO's storage on the host. It is created/filled
automatically on first run; you do not need to add anything to it.
