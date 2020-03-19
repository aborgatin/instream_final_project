For generating data you need:
 1. Build the project
    ```
    cd events-generator
    mvn clean install
    ```
 2. Generate data:
    2.1 For generating normal data you need use the command:
        ```
        java -jar target/events-generator-1.0-SNAPSHOT-jar-with-dependencies.jar -c 100 -p /work/projects/in-stream/data/
        ```
    2.2 For generating bot data you need use the command:
        ```
        java -jar target/events-generator-1.0-SNAPSHOT-jar-with-dependencies.jar -c 100 -b -d 1000 -p /work/projects/in-stream/data/
        ```
    