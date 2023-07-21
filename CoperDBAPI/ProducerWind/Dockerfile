# Set base image (host OS)
FROM python:3.10.12

# Copy the content of the project directory to the working directory
COPY . /main2

# Set the working directory in the container
WORKDIR /main2

# Install the Climate Data Operators (CDO) library
RUN apt-get update && apt-get install -y cdo

# Install any dependencies
RUN pip install -r requirements.txt

# Install any Python dependencies required by your script
RUN pip install cdsapi

# Set the directive to specify the executable that will run when the container is initiated
ENTRYPOINT [ "python" ]

# Specify the command to run on container start
CMD [ "producerWind.py" ]

