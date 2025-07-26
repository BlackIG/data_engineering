# Use official lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expose default Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "home.py", "--server.port=8501", "--server.address=0.0.0.0"]


# -- How to use --
# docker build -t broketechbro-app .
# docker run -p 8501:8501 broketechbro-app
