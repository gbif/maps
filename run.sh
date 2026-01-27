mvn clean package -pl '!mapnik-server' -DskipTests
rclone --progress copy spark-generate-maps/target/spark-generate-maps-1.1.6-SNAPSHOT.jar prod:/user/tim
kubectl --context production -n production delete -f spark-generate-maps/tim.yaml     
kubectl --context production -n production apply -f spark-generate-maps/tim.yaml     
