import sys
import subprocess

test_names = ['t3b00r20p8n3',
              't3b00r40p8n3',
              't3b00r80p8n3',
              't3b05r20p8n3',
              't3b05r40p8n3',
              't3b05r80p8n3',   # 5
              't3b10r20p8n3',
              't3b10r40p8n3',
              't3b10r80p8n3',
              't6b00r20p8n3',
              't6b00r40p8n3',   # 10
              't6b00r80p8n3',
              't6b05r20p8n3',
              't6b05r40p8n3',
              't6b05r80p8n3',
              't6b10r20p8n3',   # 15
              't6b10r40p8n3',
              't6b10r80p8n3',
              't60b00r20p8n3',  # 18
              't60b00r40p8n3',  # 19
              't60b00r80p8n3',
              't60b05r20p8n3',
              't60b05r40p8n3',  # 22
              't60b05r80p8n3',
              't60b10r20p8n3',  # 24
              't60b10r40p8n3',
              't60b10r80p8n3',  # 26

              't3b00r80p8n3',   # 27
              't3b00r120p8n3',  # 28
              't3b00r160p8n3',
              't3b05r80p8n3',   # 30
              't3b05r120p8n3',
              't3b05r160p8n3',  # 32
              't3b10r80p8n3',
              't3b10r120p8n3',  # 34
              't3b10r160p8n3',

              't1b00r100p3n2',  # 36
              't1b05r100p3n2',  # 37
              't1b10r100p3n2'   # 38
              ]
methods = ['prr', 'rr', 'fcfs', 'fair']

args = sys.argv[1:]
i_start = int(args[0])
j_start = int(args[1])
k_start = int(args[2])

i_end = int(args[3])
j_end = int(args[4])
k_end = int(args[5])

db = args[6]

for k in range(k_start, k_end + 1):
    for i in range(i_start, i_end + 1):
        for j in range(j_start, j_end + 1):

            test_name, method, number = test_names[i], methods[j], k
            cmd = 'python main.py {0} /var/scratch/rezaeian/{3} ' \
                  '.p 0 {1} {2}'.format(test_name, number, method, db)

            # print cmd

            command_list = cmd.split()
            output_string = subprocess.check_output(command_list)
            output_file = "outputs/{0}_{1:0>2}_{2}.txt".format(test_name, number, method)
            file_handle = open(output_file, 'w')
            file_handle.write(output_string)
            file_handle.close()
