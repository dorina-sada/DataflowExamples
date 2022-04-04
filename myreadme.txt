export GOOGLE_APPLICATION_CREDENTIALS="<your path for the key>"

gcloud auth application-default login

mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.click.example.BikeTrip \
    -Dexec.args="--project=sada-dorina \
    --gcpTempLocation=gs://dataflow-test111/temp/ \
    --tempLocation=gs://dataflow-test111/temp/ \
    --runner=DataflowRunner \
    --region=us-central1"


mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.click.example.StarterPipeline \
    -Dexec.args="--project=sada-dorina \
    --gcpTempLocation=gs://dataflow-test111/temp/ \
    --tempLocation=gs://dataflow-test111/temp/ \
    --runner=DataflowRunner \
    --region=us-central1"

