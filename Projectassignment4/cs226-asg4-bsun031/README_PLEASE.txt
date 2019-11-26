#0 Please MAKE SURE YOU STARTED Spark and AsterixDB.../start-sample-cluster then run the Runme.sh shell!

#1 The SolutionToAsg4.java is the source code for Spark taskA and taskB.

#2 The jar file is in the /cs226-asg4-bsun031 folder and /cs226-asg4-bsun031/out/artifacts/cs226_asg4_bsun031_jar folder

#3 The timestamp range for TaskB is hard coded as start: 804571201 end: 804571214, as both edge is included(>= and <=), so the result should be 13.

#4 Please redefine the "spark-submit" command on the 2nd line of Runme.sh if it doesnt work because of different spark environments

#5 The jar file only includes the spark part, the AsterixDB part is in the Runme.sh using "curl"

#6 For any further questions about my answer to this assignment, feel free to contact bsun031@ucr.edu :)

#7 A result of Spark taskA.txt and taskB.txt generated on my machine is in cs226-asg4-bsun031 folder, A result of AsterixDB taskA.txt, taskB.txt and taskC.txt is within the directory /cs226-asg4-bsun031/Asterix

#8 "/cs226-asg4-bsun031/cs226-asg4-bsun031/" is the Intellij Idea project folder which you can run the project using IDE

#######################################################################
Run time for all the tasks of both platorm(in ms unit):

               Spark             AsterixDB
taskA     3010ms         23.936479ms

taskB     401ms           18.694035ms

taskC     -                      19.703708ms