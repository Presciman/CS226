@Author Baixi Sun

Contents:
#1 This read me file
#2 SparkRDDasg.java : the source code of this assignment
#3 cs226-asg3-bsun031.jar : the .jar file that generated from SparkRDDasg.java
#4 task1.txt and task2.txt, the output result of my project running on my local machine
#5 A screenshot for running time observation
       Time for initializing Spark and read the data file: 3049 (ms)
       Time for processing task1: 57 (ms)
       Time for processing task2: 21 (ms)
#6 executable.sh : the script that runs the project

Description
jar name: cs226-asg3-bsun031.jar
main class: SparkRDDasg
args[0]: input file path

P.S. I tried to write a .sh script but it may cause failure due to environmental differences.
 Since we have a .jar file, one script version can be:

#!/bin/sh
spark-submit --master --class "SparkRDDasg" cs226-asg3-bsun031.jar $1

to run the bash script, use ./executable.sh <input file path>
########################################
if it doesn't work, please modify the script to run the .jar file or use the  SparkRDDasg.java in this folder!
########################################

Thanks!