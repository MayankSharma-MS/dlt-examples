# Use a base Python image
FROM python:3.12

# Set the working directory inside the container
WORKDIR /etc/dataos/work

# Copy your project files into the container
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose any necessary ports (optional)
EXPOSE 8080

# Define the default command to run your DLT pipeline
# CMD ["python", "pipeline.py"]
