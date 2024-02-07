gcloud init
gcloud auth login
gcloud auth application-default login
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable vision.googleapis.com
gcloud services enable cloudbuild.googleapis.com
python -m venv dataproject2
dataproject2\Scripts\activate.bat
pip install -U -r requirements.txt