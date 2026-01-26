# Use the official MinIO image
FROM minio/minio

#Set default credentials (so you don't have to type them every time)
# override these later using -e if needed
ENV MINIO_ROOT_USER=admin
ENV MINIO_ROOT_PASSWORD=password

# Expose the API port (9000) and the Console port (9001)
EXPOSE 9000
EXPOSE 9001

# The command to start the server (this replaces the end of your docker run command)
CMD ["server", "/data", "--console-address", ":9001"]