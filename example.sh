export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH=/opt/homebrew/opt/openjdk@17/bin/:$PATH
./gradlew clean shadowJar 
java -jar ./build/libs/iceberg-rest-image-all.jar mike:test1
