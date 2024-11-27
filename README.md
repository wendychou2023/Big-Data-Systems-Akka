# ddm-akka
Akka example and homework code for the "Big Data Systems" lecture.
Details of solution is mentioned in file Solution.pptx

## Requirements
- Java 9 >=
- Maven Compiler Version 3.1.8 >=

## Getting started
1. Clone repo
  ```
  git clone https://github.com/LyyNgg/Big-Data-class.git
  ```
        
2. Decompress test data
  ```
  cd ddm-akka/data
  unzip TPCH.zip
  ```

3. Build project with maven
  ```
  cd ..
  mvn package
  ```

4. First run
  ```
  java -jar target/ddm-akka-1.0.jar master
  ```
