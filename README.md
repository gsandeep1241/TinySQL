# TinySQL

## EXECUTION INSTRUCTIONS 
THIS IS THE README FILE. PLEASE FOLLOW THE INSTRUCTIONS BELOW TO EXECUTE.
1. Once you have unzipped the given zip file, you should see a folder named code.
2. Navigate into this folder by using the terminal
3. Inside the “code” folder, you should see the “src” folder which contains the “storageManager”.
4. This is the pure storageManager as provided. If you choose, you can replace this with your version of the storage manager
5. Once done, navigate to the “code/src” folder
6. Compile the code using “javac Main.java”
7. You should see the Main.class formed there
8. Note the presence of the “test.sql” in the code/src folder
9. It contains all the queries provided to us that satisfy the grammar (some queries do not fit the grammar- details below)
10. Place your corresponding test file in this position
11. To run, execute “java Main” from the same “src” folder
12. You should be prompted for options. Now, the program is running and you can choose the appropriate options
13. Note the output printed on terminal
14. The entire test.sql takes about 300 seconds to run

Problematic queries:
1. SELECT * FROM course WHERE NOT exam = 0
REASON: NOT operator not supported.
2. SELECT * FROM course WHERE [ exam = 100 OR homework = 100 ] AND project = 100
REASON: [ ] operator means optional in the specifications. Cannot be actually used.
3. SELECT * FROM course WHERE ( exam * 30 + homework * 20 + project * 50 ) / 100 = 100
REASON: /(division) operator not supported.
4. SELECT * FROM course WHERE grade = "C" AND [ exam > 70 OR project > 70 ] AND NOT ( exam * 30 + homework * 20 + project * 50 ) / 100 < 60
REASON: / operator not supported; [ ] not meant to be used in query.
5. SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam = 100 OR course2.exam = 100 ]
REASON: [ ] not meant to be used in query.
6. SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.homework = 100 ]
REASON: [ ] not meant to be used in query.
7. SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.grade = "A" AND course2.grade = "A" ] ORDER BY course.exam
REASON: [ ] not meant to be used in query.
8. SELECT * FROM course, course2 WHERE exam + homework = 200
REASON: exam + homework need to be surrounded by braces
