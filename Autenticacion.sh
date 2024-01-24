chmod +x ./tu_script.sh
gcloud init
gcloud auth login
gcloud auth application-default login
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable vision.googleapis.com
gcloud services enable cloudbuild.googleapis.com
python3 -m venv dataproject2
source dataproject2/bin/activate
pip install -U -r requirements.txt