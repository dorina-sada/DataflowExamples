export GOOGLE_APPLICATION_CREDENTIALS="<your path for the key>"

gcloud auth application-default login

mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.click.example.BikeTrip \
    -Dexec.args="--project=${projrct_id} \
    --gcpTempLocation=gs://${bucket_name}/temp/ \
    --tempLocation=gs://${bucket_name}/temp/ \
    --runner=DataflowRunner \
    --region=us-central1"


mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.click.example.StarterPipeline \
    -Dexec.args="--project=${projrct_id}  \
    --gcpTempLocation=gs://${bucket_name}/temp/ \
    --tempLocation=gs://${bucket_name}/temp/ \
    --runner=DataflowRunner \
    --region=us-central1"


mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.click.example.BikeTrip_postgresql \
    -Dexec.args="--project=sada-dorina \
    --gcpTempLocation=gs://dataflow-test111/temp/ \
    --tempLocation=gs://dataflow-test111/temp/ \
    --runner=DataflowRunner \
    --region=us-central1"



mvn  -Pdataflow-runner compile exec:java \
     -Dexec.mainClass=com.click.example.BikeTrip_postgresql \
     -Dexec.args=" --project=sada-dorina \
     --gcpTempLocation=gs://dataflow-test111/temp/ \
     --tempLocation=gs://dataflow-test111/temp/ \
     --runner=DataflowRunner \
     --region=us-central1"



     //--subnetwork=https://www.googleapis.com/compute/v1/projects/123/regions/us-central1/subnetworks/4bf9 "