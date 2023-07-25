# Set base image (host OS)
FROM python:3.10.12

# Copy the content of the project directory to the working directory
COPY . /main

# Set the working directory in the container
WORKDIR /main

# Install required packages
RUN apt-get update && apt-get install -y libhdf5-dev libnetcdf-dev libudunits2-dev

# Install any dependencies
RUN pip install -r requirements.txt

# Set the directive to specify the executable that will run when the container is initiated
ENTRYPOINT [ "python" ]

# Specify the command to run on container start
CMD [ "producerWave.py" ]