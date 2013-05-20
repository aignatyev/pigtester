A = load 'input.txt' using PigStorage(':');
B = foreach A generate $0 as id;
store B into '/target/result'